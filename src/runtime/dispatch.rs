//! Event dispatch pipeline

use std::sync::Arc;
use ahash::AHashMap;
use roaring::RoaringBitmap;
use crate::{UserId, WalEvent, EventKind, DispatchError, compiler::{Vm, Tri}};
use super::{
    ids::UserOrdinal,
    partition::TablePartition,
};

/// User dictionary for ordinal ↔ UserId translation
///
/// Maps dense ordinals (0-based, used in bitmaps) to sparse UserIds.
/// Enables efficient RoaringBitmap operations while supporting arbitrary UserIds.
#[derive(Clone, Debug)]
pub struct UserDictionary {
    /// UserOrdinal → UserId (dense, 0-indexed)
    ordinal_to_user: Vec<UserId>,
    /// UserId → UserOrdinal (for reverse lookup)
    user_to_ordinal: AHashMap<UserId, UserOrdinal>,
}

impl UserDictionary {
    /// Create new empty dictionary
    #[must_use]
    pub fn new() -> Self {
        Self {
            ordinal_to_user: Vec::new(),
            user_to_ordinal: AHashMap::new(),
        }
    }

    /// Get or create ordinal for user
    pub fn get_or_create(&mut self, user_id: UserId) -> UserOrdinal {
        if let Some(&ordinal) = self.user_to_ordinal.get(&user_id) {
            return ordinal;
        }

        // Allocate new ordinal
        let ordinal = UserOrdinal::new(self.ordinal_to_user.len() as u32);
        self.ordinal_to_user.push(user_id);
        self.user_to_ordinal.insert(user_id, ordinal);

        ordinal
    }

    /// Get ordinal for user (if exists)
    #[must_use]
    pub fn get(&self, user_id: UserId) -> Option<UserOrdinal> {
        self.user_to_ordinal.get(&user_id).copied()
    }

    /// Resolve ordinals to UserIds
    #[must_use]
    pub fn resolve_users(&self, ordinals: &RoaringBitmap) -> Vec<UserId> {
        let mut users: Vec<UserId> = ordinals
            .iter()
            .filter_map(|ord| self.ordinal_to_user.get(ord as usize).copied())
            .collect();

        users.sort_unstable();
        users.dedup();
        users
    }

    /// Get user by ordinal
    #[must_use]
    pub fn get_user(&self, ordinal: UserOrdinal) -> Option<UserId> {
        self.ordinal_to_user.get(ordinal.get() as usize).copied()
    }

    /// Remove user (for cleanup)
    pub fn remove(&mut self, user_id: UserId) -> Option<UserOrdinal> {
        self.user_to_ordinal.remove(&user_id)
    }

    /// Get ordinal_to_user vector for serialization
    #[must_use]
    pub fn ordinal_to_user_vec(&self) -> Vec<UserId> {
        self.ordinal_to_user.clone()
    }
}

impl Default for UserDictionary {
    fn default() -> Self {
        Self::new()
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
/// 6. Resolve users from matching predicates
pub fn dispatch_users(
    event: &WalEvent,
    partition: &TablePartition,
    user_dict: &UserDictionary,
    vm: &mut Vm,
) -> Result<Vec<UserId>, DispatchError> {
    // 1. Validate event
    validate_event(event)?;

    // 2. Get row image based on event kind
    let row = match event.kind {
        EventKind::Insert => event.new_row.as_ref()
            .ok_or(DispatchError::MissingRequiredRowImage("INSERT requires new_row"))?,
        EventKind::Update => event.new_row.as_ref()
            .ok_or(DispatchError::MissingRequiredRowImage("UPDATE requires new_row"))?,
        EventKind::Delete => event.old_row.as_ref()
            .ok_or(DispatchError::MissingRequiredRowImage("DELETE requires old_row"))?,
    };

    // 3. Select candidates (index lookups + fallback)
    let candidates = partition.select_candidates(row, event.kind, &event.changed_columns);

    // 4. VM evaluation (filter to Tri::True)
    let snapshot = partition.load_snapshot();
    let mut matching_ordinals = RoaringBitmap::new();

    for pred_id_u32 in candidates.iter() {
        let pred_id = super::ids::PredicateId::from_u32(pred_id_u32);

        if let Some(pred) = snapshot.predicates.get_predicate(pred_id) {
            // Evaluate predicate against row
            let result = vm.eval(&pred.bytecode, row)
                .map_err(|e| DispatchError::VmError(format!("{:?}", e)))?;

            // Only Tri::True is a match
            if result == Tri::True {
                // Collect user ordinals for this predicate
                if let Some(bitmap) = snapshot.predicates.predicate_users.get(&pred_id) {
                    matching_ordinals |= bitmap;
                }
            }
        }
    }

    // 5. Resolve users
    Ok(user_dict.resolve_users(&matching_ordinals))
}

/// Validate event has required fields
fn validate_event(event: &WalEvent) -> Result<(), DispatchError> {
    match event.kind {
        EventKind::Insert => {
            if event.new_row.is_none() {
                return Err(DispatchError::MissingRequiredRowImage(
                    "INSERT requires new_row"
                ));
            }
        }
        EventKind::Update => {
            if event.new_row.is_none() {
                return Err(DispatchError::MissingRequiredRowImage(
                    "UPDATE requires new_row"
                ));
            }
        }
        EventKind::Delete => {
            if event.old_row.is_none() {
                return Err(DispatchError::MissingRequiredRowImage(
                    "DELETE requires old_row"
                ));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_user_dictionary_get_or_create() {
        let mut dict = UserDictionary::new();

        let ord1 = dict.get_or_create(100);
        assert_eq!(ord1.get(), 0);

        let ord2 = dict.get_or_create(200);
        assert_eq!(ord2.get(), 1);

        // Same user returns same ordinal
        let ord1_again = dict.get_or_create(100);
        assert_eq!(ord1_again.get(), 0);
    }

    #[test]
    fn test_user_dictionary_resolve() {
        let mut dict = UserDictionary::new();

        dict.get_or_create(10);
        dict.get_or_create(20);
        dict.get_or_create(30);

        let mut bitmap = RoaringBitmap::new();
        bitmap.insert(0); // User 10
        bitmap.insert(2); // User 30

        let users = dict.resolve_users(&bitmap);
        assert_eq!(users, vec![10, 30]);
    }

    #[test]
    fn test_user_dictionary_get() {
        let mut dict = UserDictionary::new();

        dict.get_or_create(42);

        assert_eq!(dict.get(42), Some(UserOrdinal::new(0)));
        assert_eq!(dict.get(99), None);
    }

    #[test]
    fn test_validate_event_insert() {
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

        assert!(validate_event(&event).is_ok());
    }

    #[test]
    fn test_validate_event_missing_new_row() {
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
            validate_event(&event),
            Err(DispatchError::MissingRequiredRowImage(_))
        ));
    }

    #[test]
    fn test_user_dictionary_remove() {
        let mut dict = UserDictionary::new();

        let ord = dict.get_or_create(42);
        assert_eq!(dict.get(42), Some(ord));

        dict.remove(42);
        assert_eq!(dict.get(42), None);
    }

    // ========================================================================
    // Phase 2: Additional Integration Tests
    // ========================================================================

    #[test]
    fn test_user_dictionary_get_user() {
        let mut dict = UserDictionary::new();

        let ord = dict.get_or_create(100);
        assert_eq!(dict.get_user(ord), Some(100));

        // Invalid ordinal
        assert_eq!(dict.get_user(UserOrdinal::new(999)), None);
    }

    #[test]
    fn test_user_dictionary_default() {
        let dict = UserDictionary::default();
        assert_eq!(dict.get(42), None);
    }

    #[test]
    fn test_validate_event_update_missing_new_row() {
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
            validate_event(&event),
            Err(DispatchError::MissingRequiredRowImage(_))
        ));
    }

    #[test]
    fn test_validate_event_delete_missing_old_row() {
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
            validate_event(&event),
            Err(DispatchError::MissingRequiredRowImage(_))
        ));
    }

    #[test]
    fn test_user_dictionary_ordinal_to_user_vec() {
        let mut dict = UserDictionary::new();
        dict.get_or_create(10);
        dict.get_or_create(20);
        dict.get_or_create(30);

        let vec = dict.ordinal_to_user_vec();
        assert_eq!(vec, vec![10, 20, 30]);
    }

    #[test]
    fn test_user_dictionary_resolve_users_dedup() {
        let mut dict = UserDictionary::new();
        dict.get_or_create(10);
        dict.get_or_create(20);

        let mut bitmap = RoaringBitmap::new();
        bitmap.insert(0); // User 10
        bitmap.insert(0); // Duplicate (bitmap auto-dedupes)
        bitmap.insert(1); // User 20

        let users = dict.resolve_users(&bitmap);
        assert_eq!(users, vec![10, 20]); // Should be sorted and deduped
    }

    // ========================================================================
    // Phase 3C: Push to 95% - Dispatch Integration Tests
    // ========================================================================

    #[test]
    fn test_dispatch_users_update_event_matching() {
        use super::super::partition::TablePartition;
        use super::super::predicate::Predicate;
        use super::super::indexes::IndexableAtom;
        use crate::compiler::{Vm, BytecodeProgram, Instruction};

        let mut partition = TablePartition::new(1);
        let mut user_dict = UserDictionary::new();
        let mut vm = Vm::new();

        // Add a predicate: age > 18
        let pred = Predicate {
            id: super::super::ids::PredicateId::from_slab_index(0),
            hash: 0x1234,
            normalized_sql: "age > 18".into(),
            bytecode: Arc::new(BytecodeProgram::new(vec![
                Instruction::LoadColumn(1),  // age column
                Instruction::PushLiteral(crate::Cell::Int(18)),
                Instruction::GreaterThan,
            ])),
            dependency_columns: Arc::from([1u16]),
            refcount: 1,
            updated_at_unix_ms: 1000,
        };

        let pred_id = pred.id;
        partition.add_predicate(pred, vec![IndexableAtom::Fallback]);

        // Add binding
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

        // Create UPDATE event with matching row (age=25 > 18)
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
        let users = result.unwrap();
        assert!(users.contains(&42), "User 42 should match age > 18 with age=25");
    }

    #[test]
    fn test_dispatch_users_delete_event_matching() {
        use super::super::partition::TablePartition;
        use super::super::predicate::Predicate;
        use super::super::indexes::IndexableAtom;
        use crate::compiler::{Vm, BytecodeProgram, Instruction};

        let mut partition = TablePartition::new(1);
        let mut user_dict = UserDictionary::new();
        let mut vm = Vm::new();

        // Add a predicate: age < 30
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

        // Create DELETE event with matching row (age=25 < 30)
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
        let users = result.unwrap();
        assert!(users.contains(&99), "User 99 should match age < 30 with age=25");
    }

    #[test]
    fn test_dispatch_users_no_match() {
        use super::super::partition::TablePartition;
        use super::super::predicate::Predicate;
        use super::super::indexes::IndexableAtom;
        use crate::compiler::{Vm, BytecodeProgram, Instruction};

        let mut partition = TablePartition::new(1);
        let mut user_dict = UserDictionary::new();
        let mut vm = Vm::new();

        // Add a predicate: age > 50
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

        // Create event with non-matching row (age=25 is NOT > 50)
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
        let users = result.unwrap();
        assert!(users.is_empty(), "No users should match age > 50 with age=25");
    }
}
