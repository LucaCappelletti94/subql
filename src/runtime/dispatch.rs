//! Event dispatch pipeline

use super::{ids::UserOrdinal, partition::TablePartition};
use crate::{
    compiler::{Tri, Vm},
    DispatchError, EventKind, IdTypes, WalEvent,
};
use ahash::AHashMap;
use roaring::RoaringBitmap;

/// User dictionary for ordinal ↔ UserId translation
///
/// Maps dense ordinals (0-based, used in bitmaps) to sparse UserIds.
/// Enables efficient RoaringBitmap operations while supporting arbitrary UserIds.
#[derive(Clone, Debug)]
pub struct UserDictionary<I: IdTypes> {
    /// UserOrdinal → UserId (dense, 0-indexed)
    ordinal_to_user: Vec<I::UserId>,
    /// UserId → UserOrdinal (for reverse lookup)
    user_to_ordinal: AHashMap<I::UserId, UserOrdinal>,
}

impl<I: IdTypes> UserDictionary<I> {
    /// Create new empty dictionary
    #[must_use]
    pub fn new() -> Self {
        Self {
            ordinal_to_user: Vec::new(),
            user_to_ordinal: AHashMap::new(),
        }
    }

    /// Get or create ordinal for user
    pub fn get_or_create(&mut self, user_id: I::UserId) -> UserOrdinal {
        if let Some(&ordinal) = self.user_to_ordinal.get(&user_id) {
            return ordinal;
        }

        // Allocate new ordinal
        #[allow(clippy::cast_possible_truncation)]
        let ordinal = UserOrdinal::new(self.ordinal_to_user.len() as u32);
        self.ordinal_to_user.push(user_id);
        self.user_to_ordinal.insert(user_id, ordinal);

        ordinal
    }

    /// Get ordinal for user (if exists)
    #[must_use]
    pub fn get(&self, user_id: I::UserId) -> Option<UserOrdinal> {
        self.user_to_ordinal.get(&user_id).copied()
    }

    /// Get user by ordinal
    #[must_use]
    pub fn get_user(&self, ordinal: UserOrdinal) -> Option<I::UserId> {
        self.ordinal_to_user.get(ordinal.get() as usize).copied()
    }

    /// Remove user (for cleanup)
    pub fn remove(&mut self, user_id: I::UserId) -> Option<UserOrdinal> {
        self.user_to_ordinal.remove(&user_id)
    }

    /// Get ordinal_to_user vector for serialization
    #[must_use]
    pub fn ordinal_to_user_vec(&self) -> Vec<I::UserId> {
        self.ordinal_to_user.clone()
    }
}

impl<I: IdTypes> Default for UserDictionary<I> {
    fn default() -> Self {
        Self::new()
    }
}

/// Zero-allocation iterator over matched user IDs.
///
/// Owns the `RoaringBitmap` (already heap-allocated during dispatch — moving
/// it is just a pointer move) and borrows the `UserDictionary` to translate
/// ordinals into user IDs.
pub struct MatchedUsers<'a, I: IdTypes> {
    bitmap_iter: roaring::bitmap::IntoIter,
    dict: &'a UserDictionary<I>,
}

impl<I: IdTypes> Iterator for MatchedUsers<'_, I> {
    type Item = I::UserId;

    fn next(&mut self) -> Option<Self::Item> {
        for ord in self.bitmap_iter.by_ref() {
            if let Some(user_id) = self.dict.ordinal_to_user.get(ord as usize).copied() {
                return Some(user_id);
            }
        }
        None
    }
}

/// Dispatch event to interested users
///
/// Main dispatch algorithm:
/// 1. Validate event
/// 2. Select row image based on event kind
/// 3. Get table partition
/// 4. Select candidate predicates (index lookups)
/// 5. VM evaluation (filter to Tri::True)
/// 6. Return zero-alloc iterator over matched users
pub fn dispatch_users<'a, I: IdTypes>(
    event: &WalEvent,
    partition: &TablePartition<I>,
    user_dict: &'a UserDictionary<I>,
    vm: &mut Vm,
) -> Result<MatchedUsers<'a, I>, DispatchError> {
    // 1. Get row image based on event kind (validates presence)
    let row = match event.kind {
        EventKind::Insert => {
            event
                .new_row
                .as_ref()
                .ok_or(DispatchError::MissingRequiredRowImage(
                    "INSERT requires new_row",
                ))?
        }
        EventKind::Update => {
            event
                .new_row
                .as_ref()
                .ok_or(DispatchError::MissingRequiredRowImage(
                    "UPDATE requires new_row",
                ))?
        }
        EventKind::Delete => {
            event
                .old_row
                .as_ref()
                .ok_or(DispatchError::MissingRequiredRowImage(
                    "DELETE requires old_row",
                ))?
        }
    };

    // 2. Select candidates (index lookups + fallback)
    let candidates = partition.select_candidates(row, event.kind, &event.changed_columns);

    // 3. VM evaluation (filter to Tri::True)
    let snapshot = partition.load_snapshot();
    let mut matching_ordinals = RoaringBitmap::new();

    for pred_id_u32 in &candidates {
        let pred_id = super::ids::PredicateId::from_u32(pred_id_u32);

        if let Some(pred) = snapshot.predicates.get_predicate(pred_id) {
            // Hybrid path: for scan-required predicates, run cheap prefilter
            // first and skip VM if predicate cannot possibly be true.
            if pred.prefilter_plan.requires_prefilter_eval && !pred.prefilter_plan.may_match(row) {
                continue;
            }

            // Evaluate predicate against row
            let result = vm
                .eval(&pred.bytecode, row)
                .map_err(|e| DispatchError::VmError(format!("{e:?}")))?;

            // Only Tri::True is a match
            if result == Tri::True {
                // Collect user ordinals for this predicate
                if let Some(bitmap) = snapshot.predicates.predicate_users.get(&pred_id) {
                    matching_ordinals |= bitmap;
                }
            }
        }
    }

    // 4. Return zero-alloc iterator
    Ok(MatchedUsers {
        bitmap_iter: matching_ordinals.into_iter(),
        dict: user_dict,
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::needless_collect)]
mod tests {
    use super::*;
    use crate::DefaultIds;
    use std::sync::Arc;

    #[test]
    fn test_user_dictionary_get_or_create() {
        let mut dict = UserDictionary::<DefaultIds>::new();

        let ord1 = dict.get_or_create(100);
        assert_eq!(ord1.get(), 0);

        let ord2 = dict.get_or_create(200);
        assert_eq!(ord2.get(), 1);

        // Same user returns same ordinal
        let ord1_again = dict.get_or_create(100);
        assert_eq!(ord1_again.get(), 0);
    }

    #[test]
    fn test_user_dictionary_get() {
        let mut dict = UserDictionary::<DefaultIds>::new();

        dict.get_or_create(42);

        assert_eq!(dict.get(42), Some(UserOrdinal::new(0)));
        assert_eq!(dict.get(99), None);
    }

    #[test]
    fn test_dispatch_insert_valid_event() {
        use super::super::partition::TablePartition;
        use crate::compiler::Vm;

        let partition = TablePartition::<DefaultIds>::new(1);
        let user_dict = UserDictionary::<DefaultIds>::new();
        let mut vm = Vm::new();

        let event = WalEvent {
            kind: EventKind::Insert,
            table_id: 1,
            pk: crate::PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([crate::Cell::Int(1)]),
            },
            old_row: None,
            new_row: Some(crate::RowImage {
                cells: Arc::from([crate::Cell::Int(100)]),
            }),
            changed_columns: Arc::from([]),
        };

        assert!(dispatch_users(&event, &partition, &user_dict, &mut vm).is_ok());
    }

    #[test]
    fn test_dispatch_insert_missing_new_row() {
        use super::super::partition::TablePartition;
        use crate::compiler::Vm;

        let partition = TablePartition::<DefaultIds>::new(1);
        let user_dict = UserDictionary::<DefaultIds>::new();
        let mut vm = Vm::new();

        let event = WalEvent {
            kind: EventKind::Insert,
            table_id: 1,
            pk: crate::PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([crate::Cell::Int(1)]),
            },
            old_row: None,
            new_row: None,
            changed_columns: Arc::from([]),
        };

        assert!(matches!(
            dispatch_users(&event, &partition, &user_dict, &mut vm),
            Err(DispatchError::MissingRequiredRowImage(_))
        ));
    }

    #[test]
    fn test_user_dictionary_remove() {
        let mut dict = UserDictionary::<DefaultIds>::new();

        let ord = dict.get_or_create(42);
        assert_eq!(dict.get(42), Some(ord));

        dict.remove(42);
        assert_eq!(dict.get(42), None);
    }

    #[test]
    fn test_user_dictionary_get_user() {
        let mut dict = UserDictionary::<DefaultIds>::new();

        let ord = dict.get_or_create(100);
        assert_eq!(dict.get_user(ord), Some(100));

        // Invalid ordinal
        assert_eq!(dict.get_user(UserOrdinal::new(999)), None);
    }

    #[test]
    fn test_user_dictionary_default() {
        let dict = UserDictionary::<DefaultIds>::default();
        assert_eq!(dict.get(42), None);
    }

    #[test]
    fn test_dispatch_update_missing_new_row() {
        use super::super::partition::TablePartition;
        use crate::compiler::Vm;

        let partition = TablePartition::<DefaultIds>::new(1);
        let user_dict = UserDictionary::<DefaultIds>::new();
        let mut vm = Vm::new();

        let event = WalEvent {
            kind: EventKind::Update,
            table_id: 1,
            pk: crate::PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([crate::Cell::Int(1)]),
            },
            old_row: Some(crate::RowImage {
                cells: Arc::from([crate::Cell::Int(100)]),
            }),
            new_row: None,
            changed_columns: Arc::from([]),
        };

        assert!(matches!(
            dispatch_users(&event, &partition, &user_dict, &mut vm),
            Err(DispatchError::MissingRequiredRowImage(_))
        ));
    }

    #[test]
    fn test_dispatch_delete_missing_old_row() {
        use super::super::partition::TablePartition;
        use crate::compiler::Vm;

        let partition = TablePartition::<DefaultIds>::new(1);
        let user_dict = UserDictionary::<DefaultIds>::new();
        let mut vm = Vm::new();

        let event = WalEvent {
            kind: EventKind::Delete,
            table_id: 1,
            pk: crate::PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([crate::Cell::Int(1)]),
            },
            old_row: None,
            new_row: Some(crate::RowImage {
                cells: Arc::from([crate::Cell::Int(100)]),
            }),
            changed_columns: Arc::from([]),
        };

        assert!(matches!(
            dispatch_users(&event, &partition, &user_dict, &mut vm),
            Err(DispatchError::MissingRequiredRowImage(_))
        ));
    }

    #[test]
    fn test_user_dictionary_ordinal_to_user_vec() {
        let mut dict = UserDictionary::<DefaultIds>::new();
        dict.get_or_create(10);
        dict.get_or_create(20);
        dict.get_or_create(30);

        let vec = dict.ordinal_to_user_vec();
        assert_eq!(vec, vec![10, 20, 30]);
    }

    #[test]
    fn test_matched_users_iterator() {
        let mut dict = UserDictionary::<DefaultIds>::new();
        dict.get_or_create(10);
        dict.get_or_create(20);
        dict.get_or_create(30);

        let mut bitmap = RoaringBitmap::new();
        bitmap.insert(0); // User 10
        bitmap.insert(2); // User 30

        let users: Vec<_> = (MatchedUsers {
            bitmap_iter: bitmap.into_iter(),
            dict: &dict,
        })
        .collect();
        assert_eq!(users, vec![10, 30]);
    }

    #[test]
    fn test_dispatch_users_update_event_matching() {
        use super::super::indexes::IndexableAtom;
        use super::super::partition::TablePartition;
        use super::super::predicate::Predicate;
        use crate::compiler::{BytecodeProgram, Instruction, PrefilterPlan, Vm};

        let mut partition = TablePartition::<DefaultIds>::new(1);
        let mut user_dict = UserDictionary::<DefaultIds>::new();
        let mut vm = Vm::new();

        let pred = Predicate {
            id: super::super::ids::PredicateId::from_slab_index(0),
            hash: 0x1234,
            normalized_sql: "age > 18".into(),
            bytecode: Arc::new(BytecodeProgram::new(vec![
                Instruction::LoadColumn(1),
                Instruction::PushLiteral(crate::Cell::Int(18)),
                Instruction::GreaterThan,
            ])),
            dependency_columns: Arc::from([1u16]),
            index_atoms: Arc::from([IndexableAtom::Fallback]),
            prefilter_plan: Arc::new(PrefilterPlan::default()),
            refcount: 1,
            updated_at_unix_ms: 1000,
        };

        let pred_id = pred.id;
        partition.add_predicate(pred, vec![IndexableAtom::Fallback]);

        let ord = user_dict.get_or_create(42);
        let binding = super::super::predicate::Binding {
            subscription_id: 100,
            predicate_id: pred_id,
            user_id: 42,
            user_ordinal: ord,
            session_id: None,
            updated_at_unix_ms: 1000,
        };
        partition.add_binding(binding, pred_id);

        let event = WalEvent {
            kind: EventKind::Update,
            table_id: 1,
            pk: crate::PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([crate::Cell::Int(1)]),
            },
            old_row: Some(crate::RowImage {
                cells: Arc::from([crate::Cell::Int(1), crate::Cell::Int(17)]),
            }),
            new_row: Some(crate::RowImage {
                cells: Arc::from([crate::Cell::Int(1), crate::Cell::Int(25)]),
            }),
            changed_columns: Arc::from([1u16]),
        };

        let result = dispatch_users(&event, &partition, &user_dict, &mut vm);
        assert!(result.is_ok());
        let users: Vec<_> = result.unwrap().collect();
        assert!(
            users.contains(&42),
            "User 42 should match age > 18 with age=25"
        );
    }

    #[test]
    fn test_dispatch_users_delete_event_matching() {
        use super::super::indexes::IndexableAtom;
        use super::super::partition::TablePartition;
        use super::super::predicate::Predicate;
        use crate::compiler::{BytecodeProgram, Instruction, PrefilterPlan, Vm};

        let mut partition = TablePartition::<DefaultIds>::new(1);
        let mut user_dict = UserDictionary::<DefaultIds>::new();
        let mut vm = Vm::new();

        let pred = Predicate {
            id: super::super::ids::PredicateId::from_slab_index(0),
            hash: 0x5678,
            normalized_sql: "age < 30".into(),
            bytecode: Arc::new(BytecodeProgram::new(vec![
                Instruction::LoadColumn(1),
                Instruction::PushLiteral(crate::Cell::Int(30)),
                Instruction::LessThan,
            ])),
            dependency_columns: Arc::from([1u16]),
            index_atoms: Arc::from([IndexableAtom::Fallback]),
            prefilter_plan: Arc::new(PrefilterPlan::default()),
            refcount: 1,
            updated_at_unix_ms: 1000,
        };

        let pred_id = pred.id;
        partition.add_predicate(pred, vec![IndexableAtom::Fallback]);

        let ord = user_dict.get_or_create(99);
        let binding = super::super::predicate::Binding {
            subscription_id: 200,
            predicate_id: pred_id,
            user_id: 99,
            user_ordinal: ord,
            session_id: None,
            updated_at_unix_ms: 1000,
        };
        partition.add_binding(binding, pred_id);

        let event = WalEvent {
            kind: EventKind::Delete,
            table_id: 1,
            pk: crate::PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([crate::Cell::Int(1)]),
            },
            old_row: Some(crate::RowImage {
                cells: Arc::from([crate::Cell::Int(1), crate::Cell::Int(25)]),
            }),
            new_row: None,
            changed_columns: Arc::from([]),
        };

        let result = dispatch_users(&event, &partition, &user_dict, &mut vm);
        assert!(result.is_ok());
        let users: Vec<_> = result.unwrap().collect();
        assert!(
            users.contains(&99),
            "User 99 should match age < 30 with age=25"
        );
    }

    #[test]
    fn test_dispatch_users_no_match() {
        use super::super::indexes::IndexableAtom;
        use super::super::partition::TablePartition;
        use super::super::predicate::Predicate;
        use crate::compiler::{BytecodeProgram, Instruction, PrefilterPlan, Vm};

        let mut partition = TablePartition::<DefaultIds>::new(1);
        let mut user_dict = UserDictionary::<DefaultIds>::new();
        let mut vm = Vm::new();

        let pred = Predicate {
            id: super::super::ids::PredicateId::from_slab_index(0),
            hash: 0xABCD,
            normalized_sql: "age > 50".into(),
            bytecode: Arc::new(BytecodeProgram::new(vec![
                Instruction::LoadColumn(1),
                Instruction::PushLiteral(crate::Cell::Int(50)),
                Instruction::GreaterThan,
            ])),
            dependency_columns: Arc::from([1u16]),
            index_atoms: Arc::from([IndexableAtom::Fallback]),
            prefilter_plan: Arc::new(PrefilterPlan::default()),
            refcount: 1,
            updated_at_unix_ms: 1000,
        };

        let pred_id = pred.id;
        partition.add_predicate(pred, vec![IndexableAtom::Fallback]);

        let ord = user_dict.get_or_create(42);
        let binding = super::super::predicate::Binding {
            subscription_id: 300,
            predicate_id: pred_id,
            user_id: 42,
            user_ordinal: ord,
            session_id: None,
            updated_at_unix_ms: 1000,
        };
        partition.add_binding(binding, pred_id);

        let event = WalEvent {
            kind: EventKind::Insert,
            table_id: 1,
            pk: crate::PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([crate::Cell::Int(1)]),
            },
            old_row: None,
            new_row: Some(crate::RowImage {
                cells: Arc::from([crate::Cell::Int(1), crate::Cell::Int(25)]),
            }),
            changed_columns: Arc::from([]),
        };

        let result = dispatch_users(&event, &partition, &user_dict, &mut vm);
        assert!(result.is_ok());
        let users: Vec<_> = result.unwrap().collect();
        assert!(
            users.is_empty(),
            "No users should match age > 50 with age=25"
        );
    }
}
