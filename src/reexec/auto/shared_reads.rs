//! Read-shaping helpers both engines call.
//!
//! Each lives here because the sync and async engines differ only in how they
//! obtain a page, and everything after that is identical.

use super::{
    Backend, IdTypes, ReExecError, RowDelta, ScalarFamily, String, SubscriptionId, Value, Vec,
};

/// Carry a connector's read position into the event-checkpoint domain.
///
/// Identity when the two domains are the same type, which is every shipped
/// pairing (`PgLsn` reads with `PgLsn` events, and so on). `None` when one
/// side has no position domain (`NoCheckpoint`): a Maxwell-fed MySQL engine
/// reads binlog positions its events cannot spell, and a positionless seed
/// is what the install layer already handles. Two DIFFERENT real position
/// domains are a wiring mistake, caught by the debug assertion rather than
/// degraded into a silent `None`.
pub fn reconcile_checkpoint<F: crate::Checkpoint, T: crate::Checkpoint>(
    checkpoint: Option<&F>,
) -> Option<T> {
    use core::any::{Any, TypeId};
    debug_assert!(
        TypeId::of::<F>() == TypeId::of::<T>()
            || TypeId::of::<F>() == TypeId::of::<crate::NoCheckpoint>()
            || TypeId::of::<T>() == TypeId::of::<crate::NoCheckpoint>(),
        "a connector and its events speak different position domains"
    );
    checkpoint
        .and_then(|value| (value as &dyn Any).downcast_ref::<T>())
        .cloned()
}

/// Walks a key set in bounded batches.
///
/// Bounded because statement duration tracks how many keys a statement names,
/// and a caller's statement timeout applies per statement, so one unbounded
/// request puts the duration under the burst's control rather than the
/// caller's.
pub struct KeyBatches<'a, B: Backend> {
    keys: &'a [Vec<Value<B>>],
    size: usize,
    at: usize,
}

impl<'a, B: Backend> KeyBatches<'a, B> {
    pub const fn new(keys: &'a [Vec<Value<B>>], size: usize) -> Self {
        Self {
            keys,
            // Zero would never advance.
            size: if size == 0 { 1 } else { size },
            at: 0,
        }
    }
}

impl<'a, B: Backend> Iterator for KeyBatches<'a, B> {
    type Item = &'a [Vec<Value<B>>];

    fn next(&mut self) -> Option<Self::Item> {
        if self.at >= self.keys.len() {
            return None;
        }
        let start = self.at;
        let end = self.keys.len().min(start + self.size);
        self.at = end;
        Some(&self.keys[start..end])
    }
}

/// What a keyed batch read does after one page.
///
/// Named rather than returned as a bare `Option`, because both outcomes
/// are ordinary and the empty one is not a failure: a batch whose keys
/// all came back is answered.
pub enum KeyedPage<B: Backend> {
    /// This batch is answered. Stop reading it.
    Answered,
    /// Read this batch again for the keys that have not come back.
    Resume(Vec<Vec<Value<B>>>),
}

/// Absorb one page of a keyed batch read, and say whether to read again.
///
/// Shared by both engines, which differ only in how they obtain the page:
/// the synchronous one calls `read_page`, the asynchronous one awaits it.
/// Everything after that is this function, and it holds no borrow of
/// either engine, so neither has to change shape to use it.
///
/// Three rules live here, and each was written twice before:
///
/// - `seen` accumulates across pages, never per page. Resetting it would
///   let a key answered on an earlier page back into the next statement,
///   which delivers it twice and, with a stable row order, never
///   terminates: the remaining sets oscillate between the halves of the
///   batch.
/// - A page with no new keys ends the read whatever it claims about
///   there being more. This crate's own readers cannot report that
///   combination, but the connector trait has outside implementors, and
///   without this a connector that did would loop forever.
/// - Resumption is inside the batch, so the statement stays bounded by
///   it. Because `seen` accumulates, the remaining set strictly shrinks
///   and the loop ends.
pub fn absorb_keyed_page<B: Backend>(
    page: crate::reexec::RowPage<B>,
    batch: &[Vec<Value<B>>],
    key_positions: &[usize],
    columns: &mut Vec<String>,
    seen: &mut SeenKeys<B>,
    present: &mut Vec<(Vec<Value<B>>, Vec<Value<B>>)>,
) -> KeyedPage<B> {
    if columns.is_empty() {
        columns.clone_from(&page.columns);
    }
    let before = seen.recorded();
    for row in page.rows {
        let key: Vec<Value<B>> = key_positions
            .iter()
            .filter_map(|position| row.get(*position).cloned())
            .collect();
        seen.record(&key);
        present.push((key, row));
    }
    if !page.more || seen.recorded() == before {
        return KeyedPage::Answered;
    }
    let remaining: Vec<Vec<Value<B>>> = batch
        .iter()
        .filter(|key| !seen.contains(key))
        .cloned()
        .collect();
    if remaining.is_empty() {
        return KeyedPage::Answered;
    }
    KeyedPage::Resume(remaining)
}

/// The one row a grouped scalar read has to answer with.
///
/// # Errors
///
/// [`crate::AggregateInstallError::RowCount`] when the read answered
/// anything else. A grouped scalar reads one group's aggregate, so more
/// than one row, no row, or a first page that claims a second all mean
/// the statement was not the one this tier thinks it sent, and guessing
/// which row to install would install an aggregate for the wrong group.
pub fn one_grouped_row<B: Backend, Err>(
    subscription: SubscriptionId,
    page: crate::reexec::RowPage<B>,
) -> Result<Vec<Value<B>>, ReExecError<Err>> {
    if page.more || page.rows.len() != 1 {
        return Err(crate::AggregateInstallError::RowCount {
            subscription,
            rows: page.rows.len(),
        }
        .into());
    }
    Ok(page
        .rows
        .into_iter()
        .next()
        .expect("the row count was just checked"))
}

pub fn decode_grouped_seed_rows<B: Backend>(rows: &mut [Vec<Value<B>>], kinds: &[ScalarFamily]) {
    for row in rows {
        for (value, kind) in row.iter_mut().zip(kinds) {
            let raw = core::mem::replace(value, Value::Missing);
            *value = B::decode_group_value(crate::backend::ValueKind::from(*kind), raw)
                .unwrap_or(Value::Missing);
        }
    }
}

/// Keys already returned across the pages of one keyed batch.
///
/// Membership is a hash lookup over the same encoding
/// [`KeyedQuery`](crate::reexec::maintain::KeyedQuery) dedups with, so the
/// resume computation stays linear in keys and returned rows. `Value`
/// carries floats, so it has neither `Hash` nor `Ord`, and a key that
/// cannot be encoded falls back to a scan of its peers, which is correct
/// and merely slower.
pub struct SeenKeys<B: Backend> {
    encoded: hashbrown::HashSet<Vec<u8>>,
    unencodable: Vec<Vec<Value<B>>>,
    recorded: usize,
}

impl<B: Backend> SeenKeys<B> {
    pub fn new() -> Self {
        Self {
            encoded: hashbrown::HashSet::new(),
            unencodable: Vec::new(),
            recorded: 0,
        }
    }

    /// Rows recorded so far, duplicates included, for the progress check.
    pub const fn recorded(&self) -> usize {
        self.recorded
    }

    pub fn record(&mut self, key: &[Value<B>]) {
        self.recorded += 1;
        match crate::backend::encode_value_key(key) {
            Some(encoded) => {
                self.encoded.insert(encoded);
            }
            None => self.unencodable.push(key.to_vec()),
        }
    }

    pub fn contains(&self, key: &[Value<B>]) -> bool {
        crate::backend::encode_value_key(key).map_or_else(
            || self.unencodable.iter().any(|held| held == key),
            |encoded| self.encoded.contains(&encoded),
        )
    }
}

/// Turn "these keys were asked about, these rows came back" into one delta per
/// key: present is an upsert, absent is a removal.
///
/// A free function because both engines produce it from the same answer, and a
/// method on the sync engine would drag its [`Connector`] bound into the async
/// one. `columns` is turned into one shared allocation carried by every
/// upsert, and a removal carries a shared empty schema: there is no row to
/// describe.
pub fn deltas_from<I, B, C>(
    subscription_id: SubscriptionId,
    consumer_id: I::ConsumerId,
    checkpoint: Option<&C>,
    keys: &[Vec<Value<B>>],
    present: &[(Vec<Value<B>>, Vec<Value<B>>)],
    columns: Vec<String>,
) -> Vec<RowDelta<I, B, C>>
where
    I: IdTypes,
    B: Backend,
    C: crate::Checkpoint,
{
    let columns: alloc::sync::Arc<[String]> = columns.into();
    let removed: alloc::sync::Arc<[String]> = alloc::sync::Arc::from(Vec::new());
    let mut deltas = Vec::with_capacity(keys.len());
    // Which keys came back, by encoded form. `Value` carries floats so it has
    // neither `Hash` nor `Ord`, and scanning the returned rows once per key
    // asked about is the product of the two counts.
    let mut returned: hashbrown::HashSet<Vec<u8>> = hashbrown::HashSet::new();
    for (key, row) in present {
        if let Some(encoded) = crate::backend::encode_value_key(key) {
            returned.insert(encoded);
        }
        deltas.push(RowDelta {
            subscription_id,
            consumer_id,
            key: key.clone(),
            columns: alloc::sync::Arc::clone(&columns),
            row: Some(row.clone()),
            checkpoint: checkpoint.cloned(),
        });
    }
    for key in keys {
        // A key that could not be encoded falls back to the scan, which is
        // correct and merely slower, rather than being reported as removed.
        let came_back = crate::backend::encode_value_key(key).map_or_else(
            || present.iter().any(|(k, _)| k == key),
            |encoded| returned.contains(&encoded),
        );
        if !came_back {
            deltas.push(RowDelta {
                subscription_id,
                consumer_id,
                key: key.clone(),
                columns: alloc::sync::Arc::clone(&removed),
                row: None,
                checkpoint: checkpoint.cloned(),
            });
        }
    }
    deltas
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{absorb_keyed_page, one_grouped_row, KeyedPage, SeenKeys};
    use crate::backend::Postgres;
    use crate::backend::Value;
    use crate::reexec::RowPage;
    use alloc::string::String;
    use alloc::vec::Vec;

    /// The subscription these reads belong to. Which one it is does not
    /// matter here; it only travels into the refusal.
    const SUBSCRIPTION: crate::SubscriptionId = 1;

    /// One page holding `rows`, each row being one key column and one payload.
    pub fn page(rows: &[i64], more: bool) -> RowPage<Postgres> {
        RowPage {
            columns: alloc::vec![String::from("id"), String::from("status")],
            rows: rows
                .iter()
                .map(|id| alloc::vec![Value::Int(*id), Value::String("paid".into())])
                .collect(),
            more,
        }
    }

    /// The keys `ids` name, as a batch.
    pub fn batch(ids: &[i64]) -> Vec<Vec<Value<Postgres>>> {
        ids.iter().map(|id| alloc::vec![Value::Int(*id)]).collect()
    }

    /// A page that answers every key in the batch ends the read.
    #[test]
    pub fn a_complete_page_answers_the_batch() {
        let keys = batch(&[1, 2]);
        let mut columns = Vec::new();
        let mut seen = SeenKeys::new();
        let mut present = Vec::new();
        let outcome = absorb_keyed_page(
            page(&[1, 2], false),
            &keys,
            &[0],
            &mut columns,
            &mut seen,
            &mut present,
        );
        assert!(matches!(outcome, KeyedPage::Answered));
        assert_eq!(present.len(), 2, "both rows were kept");
        assert_eq!(columns, ["id", "status"], "the column names were adopted");
    }

    /// A page that claims more and answers some keys resumes on the rest.
    #[test]
    pub fn a_partial_page_resumes_on_the_keys_left() {
        let keys = batch(&[1, 2, 3]);
        let mut columns = Vec::new();
        let mut seen = SeenKeys::new();
        let mut present = Vec::new();
        let outcome = absorb_keyed_page(
            page(&[1], true),
            &keys,
            &[0],
            &mut columns,
            &mut seen,
            &mut present,
        );
        let KeyedPage::Resume(remaining) = outcome else {
            panic!("a page claiming more with keys left resumes");
        };
        assert_eq!(
            remaining,
            batch(&[2, 3]),
            "only the unanswered keys go back"
        );
    }

    /// The remaining set strictly shrinks across pages, which is what ends
    /// the loop.
    ///
    /// `seen` accumulates rather than resetting per page. Resetting it
    /// would let a key answered on an earlier page back into the next
    /// statement, which delivers it twice and, with a stable row order,
    /// never terminates: the remaining sets oscillate between the halves
    /// of the batch.
    #[test]
    pub fn seen_keys_accumulate_across_pages() {
        let keys = batch(&[1, 2, 3]);
        let mut columns = Vec::new();
        let mut seen = SeenKeys::new();
        let mut present = Vec::new();
        let first = absorb_keyed_page(
            page(&[1], true),
            &keys,
            &[0],
            &mut columns,
            &mut seen,
            &mut present,
        );
        assert!(matches!(first, KeyedPage::Resume(_)));
        let second = absorb_keyed_page(
            page(&[2], true),
            &keys,
            &[0],
            &mut columns,
            &mut seen,
            &mut present,
        );
        let KeyedPage::Resume(remaining) = second else {
            panic!("one key is still unanswered");
        };
        assert_eq!(
            remaining,
            batch(&[3]),
            "the key answered on the first page does not come back"
        );
    }

    /// A page with no new keys ends the read whatever it claims about
    /// there being more.
    ///
    /// This crate's own readers cannot report that combination, but the
    /// connector trait has outside implementors, and without this rule a
    /// connector that did would loop forever.
    #[test]
    pub fn a_page_with_no_new_keys_ends_the_read() {
        let keys = batch(&[1, 2]);
        let mut columns = Vec::new();
        let mut seen = SeenKeys::new();
        let mut present = Vec::new();
        let outcome = absorb_keyed_page(
            page(&[], true),
            &keys,
            &[0],
            &mut columns,
            &mut seen,
            &mut present,
        );
        assert!(
            matches!(outcome, KeyedPage::Answered),
            "an empty page that claims more still ends the read"
        );
    }

    /// A grouped scalar read answers exactly one row, and anything else is
    /// refused rather than guessed at.
    #[test]
    pub fn a_grouped_read_takes_exactly_one_row() {
        let one = one_grouped_row::<Postgres, ()>(SUBSCRIPTION, page(&[7], false));
        assert_eq!(
            one.unwrap(),
            alloc::vec![Value::Int(7), Value::String("paid".into())],
            "one row is the answer"
        );
        assert!(
            one_grouped_row::<Postgres, ()>(SUBSCRIPTION, page(&[7, 8], false)).is_err(),
            "two rows are not one group's aggregate"
        );
        assert!(
            one_grouped_row::<Postgres, ()>(SUBSCRIPTION, page(&[], false)).is_err(),
            "no row is not one group's aggregate either"
        );
        assert!(
            one_grouped_row::<Postgres, ()>(SUBSCRIPTION, page(&[7], true)).is_err(),
            "a first page claiming a second was not the statement this tier sent"
        );
    }
}
