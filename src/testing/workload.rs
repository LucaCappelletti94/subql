//! Deterministic benchmark and profiling workload.
//!
//! One definition, used by the criterion dispatch benchmark and by the
//! `dhat-heap` memory profile. It lives here, ungated, because
//! `crate::memory_profile_workload` is behind the `dhat-heap` feature
//! while the `dispatch` bench declares no required features, so the bench
//! compiles the library without that feature and cannot see that module.
//!
//! Hidden from the rendered docs: these items are `pub` only so an
//! external bench target can reach them.
//!
//! Only the seed machinery and the predicate generators are shared. The
//! two callers keep their own catalog and event fixtures on purpose: the
//! bench schema carries an eleventh `folded TEXT COLLATE "C"` column its
//! pattern-dispatch benchmark needs, and the profile's ten-column shape is
//! the calibrated dhat workload.
#![allow(clippy::unreadable_literal)]

use alloc::format;
use alloc::string::String;

const STATUS_BUCKETS: [&str; 7] = [
    "pending",
    "active",
    "paid",
    "shipped",
    "cancelled",
    "fraud_hold",
    "backorder",
];

#[must_use]
pub const fn mix_seed(mut value: u64) -> u64 {
    // SplitMix64 finalizer: deterministic pseudo-randomness for stable benches.
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58476d1ce4e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d049bb133111eb);
    value ^ (value >> 31)
}

#[must_use]
pub fn bounded_i64(seed: u64, modulo: u64) -> i64 {
    i64::try_from(mix_seed(seed) % modulo).unwrap_or(0)
}

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

#[must_use]
pub fn realistic_tree_sql(seed: u64) -> String {
    match mix_seed(seed) % 10 {
        0..=2 => equality_tree_sql(seed),
        3..=6 => range_tree_sql(seed),
        7..=8 => mixed_tree_sql(seed),
        _ => fallback_tree_sql(seed),
    }
}

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
