//! Shared workload fixtures for the `dispatch` criterion benchmark and the
//! `memory_profile` binary, gated by the `testing` feature.
//!
//! The two callers once carried one byte-identical generator core plus two
//! deliberately different tail shapes, so the core lives here once and both
//! tail shapes stay. [`bench_catalog`] matches the `memory_profile` workload
//! at ten columns. [`bench_catalog_folded`] adds the `folded TEXT
//! COLLATE "C"` column the pattern benchmarks read, and its rows carry eleven
//! values through [`make_test_event_folded`]. Collapsing the two shapes would
//! move the dhat profile numbers, so the arity split is pinned by the tests
//! below rather than assumed.

use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;

use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;

use super::TestEvent;
use crate::backend::{Postgres, Value};

/// The seven order-status strings the seeded workload cycles through.
pub const STATUS_BUCKETS: [&str; 7] = [
    "pending",
    "active",
    "paid",
    "shipped",
    "cancelled",
    "fraud_hold",
    "backorder",
];

/// SplitMix64 finalizer, the deterministic pseudo-random source behind every
/// seeded fixture value.
#[must_use]
pub const fn mix_seed(mut value: u64) -> u64 {
    // SplitMix64 finalizer: deterministic pseudo-randomness for stable benches.
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

/// A deterministic value in `0..modulo` derived from `seed`.
///
/// # Panics
///
/// Panics when `modulo` is zero, like any remainder by zero.
#[must_use]
pub fn bounded_i64(seed: u64, modulo: u64) -> i64 {
    i64::try_from(mix_seed(seed) % modulo).unwrap_or(0)
}

/// One of [`STATUS_BUCKETS`] chosen by `seed`.
#[must_use]
pub const fn status_for(seed: u64) -> &'static str {
    match mix_seed(seed) % 7 {
        0 => STATUS_BUCKETS[0],
        1 => STATUS_BUCKETS[1],
        2 => STATUS_BUCKETS[2],
        3 => STATUS_BUCKETS[3],
        4 => STATUS_BUCKETS[4],
        5 => STATUS_BUCKETS[5],
        _ => STATUS_BUCKETS[6],
    }
}

/// A five-predicate `WHERE` of equality and range terms for `seed`, the
/// shape the equality dispatch tier is measured on.
#[must_use]
pub fn equality_tree_sql(seed: u64) -> String {
    let amount = 25 + bounded_i64(seed ^ 0x11, 600);
    let priority = 1 + bounded_i64(seed ^ 0x22, 9);
    let shipping = 5 + bounded_i64(seed ^ 0x33, 20);
    let tax = 2 + bounded_i64(seed ^ 0x44, 30);
    let user_bucket = bounded_i64(seed ^ 0x55, 20_000);
    let status_a = status_for(seed ^ 0x66);
    let status_b = status_for(seed ^ 0x77);

    format!(
        "SELECT * FROM orders WHERE \
         (((amount = {amount} AND status = '{status_a}') \
           OR (user_id = {user_bucket} AND priority = {priority})) \
          AND shipping = {shipping}) \
         OR (status = '{status_b}' AND tax = {tax})"
    )
}

/// A range-heavy five-predicate `WHERE` for `seed`.
#[must_use]
pub fn range_tree_sql(seed: u64) -> String {
    let min_amount = 50 + bounded_i64(seed ^ 0x111, 1_500);
    let max_amount = min_amount + 40 + bounded_i64(seed ^ 0x222, 500);
    let priority_floor = 1 + bounded_i64(seed ^ 0x333, 9);
    let qty_min = 1 + bounded_i64(seed ^ 0x444, 25);
    let qty_max = qty_min + 3 + bounded_i64(seed ^ 0x555, 20);
    let created_after = 1_700_000_000 + bounded_i64(seed ^ 0x666, 120 * 24 * 3600);
    let status_a = status_for(seed ^ 0x777);
    let status_b = status_for(seed ^ 0x888);

    format!(
        "SELECT * FROM orders WHERE \
         (((amount BETWEEN {min_amount} AND {max_amount}) AND priority >= {priority_floor}) \
           OR (created_at >= {created_after} AND quantity BETWEEN {qty_min} AND {qty_max})) \
         AND (status IN ('{status_a}', '{status_b}') OR discount IS NULL)"
    )
}

/// A mix of equality, range, and `OR`-branch predicates for `seed`.
#[must_use]
pub fn mixed_tree_sql(seed: u64) -> String {
    let amount_floor = 120 + bounded_i64(seed ^ 0x1010, 2_000);
    let amount_ceiling = amount_floor + 40 + bounded_i64(seed ^ 0x2020, 900);
    let priority_floor = 1 + bounded_i64(seed ^ 0x3030, 9);
    let quantity_floor = 1 + bounded_i64(seed ^ 0x4040, 35);
    let created_from = 1_698_000_000 + bounded_i64(seed ^ 0x5050, 180 * 24 * 3600);
    let created_to = created_from + 8 * 24 * 3600 + bounded_i64(seed ^ 0x6060, 20 * 24 * 3600);
    let discount_cap = bounded_i64(seed ^ 0x7070, 15);
    let modulus = 7 + bounded_i64(seed ^ 0x8080, 17);
    let residue = bounded_i64(seed ^ 0x9090, u64::try_from(modulus).unwrap_or(1));
    let status_a = status_for(seed ^ 0xAAAA);
    let status_b = status_for(seed ^ 0xBBBB);

    format!(
        "SELECT * FROM orders WHERE \
         ((((amount > {amount_floor} AND amount < {amount_ceiling}) \
            AND (status = '{status_a}' OR status = '{status_b}')) \
           AND (priority >= {priority_floor} OR quantity >= {quantity_floor})) \
          OR ((discount IS NULL OR discount <= {discount_cap}) \
              AND created_at BETWEEN {created_from} AND {created_to})) \
         AND (user_id % {modulus} = {residue})"
    )
}

/// A query whose leading predicates match little, forcing the planner-style
/// fallback path for `seed`.
#[must_use]
pub fn fallback_tree_sql(seed: u64) -> String {
    let value_floor = 400 + bounded_i64(seed ^ 0xAAAA_1111, 12_000);
    let shipping_tax_floor = 10 + bounded_i64(seed ^ 0xBBBB_2222, 250);
    let priority_floor = 1 + bounded_i64(seed ^ 0xCCCC_3333, 9);
    let created_from = 1_699_000_000 + bounded_i64(seed ^ 0xDDDD_4444, 210 * 24 * 3600);
    let created_to = created_from + 3 * 24 * 3600 + bounded_i64(seed ^ 0xEEEE_5555, 45 * 24 * 3600);
    let amount_floor = 90 + bounded_i64(seed ^ 0xFFFF_6666, 1_500);
    let status_prefix = match mix_seed(seed ^ 0xABCD) % 4 {
        0 => "act",
        1 => "pend",
        2 => "ship",
        _ => "fraud",
    };

    format!(
        "SELECT * FROM orders WHERE \
         ((((amount * quantity) > {value_floor}) AND status ILIKE '{status_prefix}%') \
           OR ((shipping + tax) > {shipping_tax_floor} \
               AND created_at BETWEEN {created_from} AND {created_to})) \
         AND (priority >= {priority_floor} OR amount > {amount_floor})"
    )
}

/// A realistic wide `WHERE` for `seed`, used by the whole-workload benches.
#[must_use]
pub fn realistic_tree_sql(seed: u64) -> String {
    match mix_seed(seed) % 10 {
        0..=2 => equality_tree_sql(seed),
        3..=6 => range_tree_sql(seed),
        7..=8 => mixed_tree_sql(seed),
        _ => fallback_tree_sql(seed),
    }
}

/// The seed for subscription `subscription_ix` in the realistic workload.
#[must_use]
pub const fn realistic_workload_seed(subscription_ix: u64) -> u64 {
    // Most subscribers belong to repeatable "hot" cohorts, with a long-tail.
    let hot_cohort = subscription_ix % 2_048;
    let long_tail = mix_seed(subscription_ix ^ 0xA53A_9E37_79B9_7F4A);
    if subscription_ix.is_multiple_of(5) {
        long_tail
    } else {
        hot_cohort
    }
}

/// The `memory_profile` catalog: ten columns, no padding before `orders`
/// except one placeholder table, so the orders table id stays at 1 (matching
/// the hardcoded `TestEvent::<Postgres>::insert(1, ...)`).
///
/// # Panics
///
/// Panics when the fixture DDL stops parsing.
#[must_use]
pub fn bench_catalog() -> ParserDB {
    ParserDB::parse::<PostgreSqlDialect>(BASE_ORDERS_DDL).expect("bench fixture DDL parses")
}

/// The `dispatch` benchmark catalog: [`bench_catalog`] plus the `folded`
/// `COLLATE "C"` column the pattern benchmarks read.
///
/// The id layout is the same, so rows from [`make_test_event_folded`] still
/// target table 1.
///
/// # Panics
///
/// Panics when the fixture DDL stops parsing.
#[must_use]
pub fn bench_catalog_folded() -> ParserDB {
    ParserDB::parse::<PostgreSqlDialect>(FOLDED_ORDERS_DDL).expect("bench fixture DDL parses")
}

const BASE_ORDERS_DDL: &str = "CREATE TABLE _bench_pad (id INT);\n\
     CREATE TABLE orders (\
         id INT PRIMARY KEY, user_id INT, amount INT, status TEXT, \
         priority INT, quantity INT, discount INT, tax INT, shipping INT, \
         created_at INT\
     );";

const FOLDED_ORDERS_DDL: &str = "CREATE TABLE _bench_pad (id INT);\n\
     CREATE TABLE orders (\
         id INT PRIMARY KEY, user_id INT, amount INT, status TEXT, \
         priority INT, quantity INT, discount INT, tax INT, shipping INT, \
         created_at INT, folded TEXT COLLATE \"C\"\
     );";

/// The ten base values one seeded `orders` row carries, shared by the
/// insert, update and delete builders so every event shape derives the
/// same row from the same seed.
#[derive(Clone)]
pub struct OrderRow {
    pub id: i64,
    pub user_id: i64,
    pub amount: i64,
    pub status: &'static str,
    pub priority: i64,
    pub quantity: i64,
    pub discount: Value<Postgres>,
    pub tax: i64,
    pub shipping: i64,
    pub created_at: i64,
}

/// Derive the base row for `seed`, one salt per column.
#[must_use]
pub fn order_row(seed: u64) -> OrderRow {
    OrderRow {
        id: 1 + bounded_i64(seed ^ 0x1A2A, 500_000),
        user_id: bounded_i64(seed ^ 0x2B3B, 20_000),
        amount: 30 + bounded_i64(seed ^ 0x3C4C, 3_500),
        status: status_for(seed ^ 0xBECF),
        priority: 1 + bounded_i64(seed ^ 0x4D5D, 9),
        quantity: 1 + bounded_i64(seed ^ 0x5E6E, 40),
        discount: if mix_seed(seed ^ 0x6F7F).is_multiple_of(5) {
            Value::<Postgres>::Null
        } else {
            Value::<Postgres>::Int(bounded_i64(seed ^ 0x7A8A, 18))
        },
        tax: 2 + bounded_i64(seed ^ 0x8B9B, 40),
        shipping: 4 + bounded_i64(seed ^ 0x9CAC, 30),
        created_at: 1_699_500_000 + bounded_i64(seed ^ 0xADBD, 240 * 24 * 3600),
    }
}

/// The row's values in column order, optionally extended with the `folded`
/// column for [`bench_catalog_folded`]-shaped catalogs.
#[must_use]
pub fn order_values(row: &OrderRow, folded: Option<&str>) -> Vec<Value<Postgres>> {
    let mut values = vec![
        Value::Int(row.id),
        Value::Int(row.user_id),
        Value::Int(row.amount),
        Value::String(row.status.into()),
        Value::Int(row.priority),
        Value::Int(row.quantity),
        row.discount.clone(),
        Value::Int(row.tax),
        Value::Int(row.shipping),
        Value::Int(row.created_at),
    ];
    if let Some(folded) = folded {
        values.push(Value::String(folded.into()));
    }
    values
}

/// Ten-value insert row matching [`bench_catalog`].
#[must_use]
pub fn make_test_event(seed: u64) -> TestEvent<Postgres> {
    TestEvent::<Postgres>::insert(1, order_values(&order_row(seed), None)).with_pk_columns([0u16])
}

/// Eleven-value insert row matching [`bench_catalog_folded`], carrying the
/// status string through the `folded` column as well.
#[must_use]
pub fn make_test_event_folded(seed: u64) -> TestEvent<Postgres> {
    let row = order_row(seed);
    let status = row.status;
    TestEvent::<Postgres>::insert(1, order_values(&row, Some(status))).with_pk_columns([0u16])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::EventKind;

    #[test]
    fn realistic_tree_sql_is_deterministic_for_seed() {
        let first = realistic_tree_sql(42);
        let second = realistic_tree_sql(42);
        assert_eq!(first, second);
        assert!(first.starts_with("SELECT * FROM orders WHERE"));
    }

    #[test]
    fn core_arithmetic_is_golden() {
        // The shared core feeds bench numbers that must not drift between
        // the criterion run and the dhat binary. Self-consistency proves
        // nothing here, so the values are pinned.
        assert_eq!(mix_seed(1), 6_238_072_747_940_578_789);
        assert_eq!(mix_seed(12_345), 17_540_659_726_606_785_873);
        assert_eq!(bounded_i64(42, 600), 362);
        assert_eq!(status_for(0), "pending");
        assert_eq!(status_for(9), "cancelled");
        assert_eq!(realistic_workload_seed(3), 3);
        assert_eq!(realistic_workload_seed(5), 6_348_343_260_619_961_841);
        assert_eq!(realistic_workload_seed(2_049), 1);
    }

    #[test]
    fn make_test_event_is_deterministic_for_seed() {
        let event_a = make_test_event(1234);
        let event_b = make_test_event(1234);
        assert_eq!(event_a.kind, EventKind::Insert);
        assert_eq!(event_a.table_id, 1);
        assert_eq!(event_a.pk_columns, event_b.pk_columns);
        assert_eq!(event_a.new_row, event_b.new_row);
    }

    #[test]
    fn each_catalog_shape_takes_exactly_its_own_row_arity() {
        use crate::catalog_helpers::{table_arity, table_id};

        // Unifying the two catalogs silently moves the dhat profile numbers,
        // so the arity split is pinned rather than assumed.
        assert_eq!(make_test_event(7).new_row.len(), 10);
        assert_eq!(make_test_event_folded(7).new_row.len(), 11);
        let base = bench_catalog();
        let folded = bench_catalog_folded();
        let base_id = table_id::<Postgres, _>(&base, "orders").expect("orders resolves");
        let folded_id = table_id::<Postgres, _>(&folded, "orders").expect("orders resolves");
        assert_eq!(table_arity(&base, base_id).expect("base arity"), 10);
        assert_eq!(table_arity(&folded, folded_id).expect("folded arity"), 11);
    }
}
