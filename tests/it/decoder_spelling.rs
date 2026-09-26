//! Holds the wal2json and Maxwell encoder spellings to live databases.
//!
//! 300 rows drawn from a deterministic xorshift seed are inserted and their
//! CDC values compared against PgCell::wal2json() and MyCell::maxwell.
//! Run with `--run-ignored ignored-only -E 'test(/decoder_spelling/)'`.

#![allow(clippy::unwrap_used)]

use crate::common;

use std::collections::BTreeMap;
use std::fmt::Write as _;

use arbitrary::Unstructured;
use diesel::{sql_query, RunQueryDsl};
use serde_json::Value as JsonValue;
use subql::test_harnesses::decoder_roundtrip::{MyCell, MyColumn, PgCell, PgColumn};

const PG_COLUMNS: &[PgColumn] = &[
    PgColumn::Bool,
    PgColumn::Int2,
    PgColumn::Int4,
    PgColumn::Int8,
    PgColumn::Float4,
    PgColumn::Float8,
    PgColumn::Numeric,
    PgColumn::Numeric20x6,
    PgColumn::Text,
    PgColumn::Varchar40,
    PgColumn::Char8,
    PgColumn::Bytea,
    PgColumn::Uuid,
    PgColumn::Timestamp,
    PgColumn::TimestampTz,
    PgColumn::Date,
    PgColumn::Time,
    PgColumn::TimeTz,
    PgColumn::Json,
    PgColumn::Jsonb,
];

const MY_COLUMNS: &[MyColumn] = &[
    MyColumn::Boolean,
    MyColumn::TinyInt,
    MyColumn::Int,
    MyColumn::IntUnsigned,
    MyColumn::BigInt,
    MyColumn::BigIntUnsigned,
    MyColumn::Float,
    MyColumn::Double,
    MyColumn::Decimal20x6,
    MyColumn::Varchar40,
    MyColumn::Text,
    MyColumn::Char8,
    MyColumn::Binary16,
    MyColumn::Varbinary40,
    MyColumn::Blob,
    MyColumn::Datetime,
    MyColumn::Datetime6,
    MyColumn::Timestamp6,
    MyColumn::Date,
    MyColumn::Time6,
    MyColumn::Json,
];

const ROW_COUNT: usize = 300;
// 8 KiB covers all 20-21 column types per row with bytes to spare.
const BYTES_PER_ROW: usize = 8_192;

fn xorshift_bytes(seed: u64, n: usize) -> Vec<u8> {
    let mut state = seed;
    let mut out = Vec::with_capacity(n);
    while out.len() < n {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        out.extend_from_slice(&state.to_le_bytes());
    }
    out.truncate(n);
    out
}

fn pg_quote(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

fn tz_matches(text: &str, zone_offset: &str) -> bool {
    let base = text.strip_suffix(" BC").unwrap_or(text);
    // rfind finds the zone sign, skipping the date hyphens before it.
    base.rfind(['+', '-'])
        .is_some_and(|pos| &base[pos..] == zone_offset)
}

// Mismatches store indices into all_rows so no cell values are copied at collection time.
#[derive(Default)]
struct ColStats {
    compared: usize,
    tz_skipped: usize,
    mismatches: Vec<(usize, usize, JsonValue)>, // (row_idx, col_idx, actual)
}

fn fmt_pg_col(label: &str, stats: &ColStats, all_rows: &[(usize, Vec<PgCell>)]) -> String {
    let n = stats.mismatches.len();
    let mut s = if stats.tz_skipped > 0 {
        format!(
            "  {label}: {} compared, {} skipped, {n} mismatches\n",
            stats.compared, stats.tz_skipped
        )
    } else {
        format!("  {label}: {} compared, {n} mismatches\n", stats.compared)
    };
    for (row_idx, col_idx, actual) in stats.mismatches.iter().take(5) {
        let (_, cells) = &all_rows[*row_idx];
        let cell = &cells[*col_idx];
        let sent = cell.text.as_deref().unwrap_or("NULL");
        let expected = cell.wal2json();
        let _ = writeln!(s, "    sent={sent:?}  expected={expected}  actual={actual}");
    }
    if n > 5 {
        let _ = writeln!(s, "    ... and {} more", n - 5);
    }
    s
}

fn fmt_my_col(label: &str, stats: &ColStats, all_rows: &[(usize, Vec<MyCell>)]) -> String {
    let n = stats.mismatches.len();
    let mut s = format!("  {label}: {} compared, {n} mismatches\n", stats.compared);
    for (row_idx, col_idx, actual) in stats.mismatches.iter().take(5) {
        let (_, cells) = &all_rows[*row_idx];
        let cell = &cells[*col_idx];
        let _ = writeln!(
            s,
            "    sent={:?}  expected={}  actual={actual}",
            cell.literal, cell.maxwell
        );
    }
    if n > 5 {
        let _ = writeln!(s, "    ... and {} more", n - 5);
    }
    s
}

fn pg_table_ddl() -> String {
    let cols: Vec<String> = PG_COLUMNS
        .iter()
        .enumerate()
        .map(|(i, col)| format!("c{i} {}", col.ddl()))
        .collect();
    format!(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, {})",
        cols.join(", ")
    )
}

fn my_table_ddl() -> String {
    let cols: Vec<String> = MY_COLUMNS
        .iter()
        .enumerate()
        .map(|(i, col)| format!("c{i} {}", col.ddl()))
        .collect();
    format!(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, {})",
        cols.join(", ")
    )
}

fn generate_pg_rows() -> Vec<(usize, Vec<PgCell>)> {
    let mut rows = Vec::with_capacity(ROW_COUNT);
    for k in 0..ROW_COUNT {
        // k stays below ROW_COUNT, so k + 1 fits in u32 and widens to u64 losslessly.
        let seed = u64::from(u32::try_from(k + 1).expect("row index fits u32"));
        let bytes = xorshift_bytes(seed, BYTES_PER_ROW);
        let mut u = Unstructured::new(&bytes);
        let mut cells = Vec::with_capacity(PG_COLUMNS.len());
        let mut ok = true;
        for &col in PG_COLUMNS {
            if let Ok(cell) = PgCell::arbitrary(col, &mut u) {
                cells.push(cell);
            } else {
                ok = false;
                break;
            }
        }
        if ok {
            rows.push((k, cells));
        }
    }
    rows
}

fn generate_my_rows() -> Vec<(usize, Vec<MyCell>)> {
    let mut rows = Vec::with_capacity(ROW_COUNT);
    for k in 0..ROW_COUNT {
        // k stays below ROW_COUNT, so k + 1 fits in u32 and widens to u64 losslessly.
        let seed = u64::from(u32::try_from(k + 1).expect("row index fits u32"));
        let bytes = xorshift_bytes(seed, BYTES_PER_ROW);
        let mut u = Unstructured::new(&bytes);
        let mut cells = Vec::with_capacity(MY_COLUMNS.len());
        let mut ok = true;
        for &col in MY_COLUMNS {
            if let Ok(cell) = MyCell::arbitrary(col, &mut u) {
                cells.push(cell);
            } else {
                ok = false;
                break;
            }
        }
        if ok {
            rows.push((k, cells));
        }
    }
    rows
}

#[test]
#[ignore = "requires Docker; run with --ignored"]
#[allow(clippy::too_many_lines)]
fn pg_wal2json_decoder_spelling() {
    common::assert_docker_available();
    let db = common::pg_database();
    let mut conn = db.connect();
    let slot = db.slot("dec_spell");

    // CREATE TABLE has no DSL form.
    sql_query(pg_table_ddl())
        .execute(&mut conn)
        .expect("CREATE TABLE t");
    common::create_slot(&mut conn, &slot);

    let all_rows = generate_pg_rows();
    let zones: &[(&'static str, &'static str)] =
        &[("UTC", "+00"), ("Etc/GMT-1", "+01"), ("Etc/GMT+5", "-05")];

    let mut pg_stats: BTreeMap<&'static str, ColStats> = BTreeMap::new();
    let mut tz_stats: BTreeMap<(&'static str, &'static str), ColStats> = BTreeMap::new();
    let mut rejected: Vec<String> = Vec::new();

    let col_list: String = (0..PG_COLUMNS.len())
        .map(|i| format!("c{i}"))
        .collect::<Vec<_>>()
        .join(", ");

    for (zi, &(zone, zone_offset)) in zones.iter().enumerate() {
        // SET has no DSL form, and wal2json prints TIMESTAMPTZ in this zone.
        sql_query(format!("SET timezone = '{zone}'"))
            .execute(&mut conn)
            .expect("SET timezone");

        if zi > 0 {
            // A TRUNCATE lands in the slot as action "T", which the insert check skips.
            sql_query("TRUNCATE t")
                .execute(&mut conn)
                .expect("TRUNCATE t");
        }

        let mut inserted_indices: Vec<usize> = Vec::new();
        for (idx, (k, cells)) in all_rows.iter().enumerate() {
            let values: Vec<String> = cells
                .iter()
                .map(|c| {
                    c.text
                        .as_deref()
                        .map_or_else(|| "NULL".to_string(), pg_quote)
                })
                .collect();
            // The column types are drawn at run time, and the typed DSL needs a schema at compile time.
            let sql = format!(
                "INSERT INTO t (id, {col_list}) VALUES ({k}, {})",
                values.join(", ")
            );
            match sql_query(&sql).execute(&mut conn) {
                Ok(_) => inserted_indices.push(idx),
                Err(e) => {
                    for cell in cells {
                        if let Some(text) = &cell.text {
                            rejected
                                .push(format!("zone={zone} {:?} text={text:?}: {e}", cell.column));
                        }
                    }
                }
            }
        }

        let raw = common::drain_slot(&mut conn, &slot);

        let wal_rows: Vec<BTreeMap<String, JsonValue>> = raw
            .iter()
            .filter_map(|line| {
                let v: JsonValue = serde_json::from_str(line).ok()?;
                let obj = v.as_object()?;
                if obj.get("action")?.as_str()? != "I" {
                    return None;
                }
                if obj.get("table")?.as_str()? != "t" {
                    return None;
                }
                Some(
                    obj.get("columns")?
                        .as_array()?
                        .iter()
                        .filter_map(|c| {
                            let name = c.get("name")?.as_str()?.to_owned();
                            let value = c.get("value").cloned().unwrap_or(JsonValue::Null);
                            Some((name, value))
                        })
                        .collect(),
                )
            })
            .collect();

        assert_eq!(
            wal_rows.len(),
            inserted_indices.len(),
            "zone {zone}: wal2json row count ({}) differs from inserted count ({})",
            wal_rows.len(),
            inserted_indices.len()
        );

        for (mut wal_row, &row_idx) in wal_rows.into_iter().zip(inserted_indices.iter()) {
            let (_, cells) = &all_rows[row_idx];
            for (col_idx, cell) in cells.iter().enumerate() {
                let col_ddl = cell.column.ddl();
                let is_tstz = cell.column == PgColumn::TimestampTz;

                let skip = if is_tstz {
                    match &cell.text {
                        // NULL does not depend on the zone, so it is compared once, in zone 0.
                        None => zi > 0,
                        Some(text) if !tz_matches(text, zone_offset) => {
                            tz_stats.entry((col_ddl, zone)).or_default().tz_skipped += 1;
                            true
                        }
                        _ => false,
                    }
                } else {
                    zi > 0 // non-TIMESTAMPTZ: UTC pass only to avoid triple-reporting.
                };

                if skip {
                    continue;
                }

                let col_name = format!("c{col_idx}");
                let actual = wal_row.remove(&col_name).unwrap_or(JsonValue::Null);
                let expected = cell.wal2json();
                let is_mismatch = expected != actual;

                if is_tstz {
                    let entry = tz_stats.entry((col_ddl, zone)).or_default();
                    entry.compared += 1;
                    if is_mismatch {
                        entry.mismatches.push((row_idx, col_idx, actual));
                    }
                } else {
                    let entry = pg_stats.entry(col_ddl).or_default();
                    entry.compared += 1;
                    if is_mismatch {
                        entry.mismatches.push((row_idx, col_idx, actual));
                    }
                }
            }
        }
    }

    let total: usize = pg_stats.values().map(|s| s.mismatches.len()).sum::<usize>()
        + tz_stats.values().map(|s| s.mismatches.len()).sum::<usize>();

    let mut report = format!(
        "=== PG wal2json decoder spelling ===\n\
         Rows attempted: {ROW_COUNT}, generated: {}\n\
         Zones: UTC (+00), Etc/GMT-1 (+01), Etc/GMT+5 (-05)\n\n",
        all_rows.len()
    );
    if !rejected.is_empty() {
        let _ = writeln!(report, "Rejected inserts ({}):", rejected.len());
        for r in rejected.iter().take(40) {
            let _ = writeln!(report, "  {r}");
        }
        report.push('\n');
    }
    report.push_str("Column comparison summary:\n");
    for &col in PG_COLUMNS {
        let ddl = col.ddl();
        if col == PgColumn::TimestampTz {
            for &(zone, zone_offset) in zones {
                let default = ColStats::default();
                let stats = tz_stats.get(&(ddl, zone)).unwrap_or(&default);
                report.push_str(&fmt_pg_col(
                    &format!("TIMESTAMPTZ [{zone} {zone_offset}]"),
                    stats,
                    &all_rows,
                ));
            }
        } else {
            let default = ColStats::default();
            let stats = pg_stats.get(ddl).unwrap_or(&default);
            report.push_str(&fmt_pg_col(ddl, stats, &all_rows));
        }
    }
    let _ = writeln!(report, "\nTotal mismatches: {total}");

    assert_eq!(total, 0, "{report}");
}

#[test]
#[ignore = "requires Docker; run with --ignored"]
#[allow(clippy::too_many_lines)]
fn mysql_maxwell_decoder_spelling() {
    common::assert_docker_available();

    let maxwell_dir = tempfile::tempdir().expect("temp dir for Maxwell output");
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(maxwell_dir.path(), std::fs::Permissions::from_mode(0o777))
            .expect("make Maxwell output dir world-writable");
    }
    let out = maxwell_dir
        .path()
        .to_str()
        .expect("Maxwell output path is UTF-8")
        .to_owned();

    let db = common::mysql_database();
    let _maxwell = common::start_maxwell(&db, &out);

    let mut my = db.connect();
    // SET has no DSL form, and UTC keeps TIMESTAMP from shifting on read-back.
    sql_query("SET time_zone = '+00:00'")
        .execute(&mut my)
        .expect("SET time_zone");
    // CREATE TABLE has no DSL form.
    sql_query(my_table_ddl())
        .execute(&mut my)
        .expect("CREATE TABLE t");

    let all_rows = generate_my_rows();
    let col_list: String = (0..MY_COLUMNS.len())
        .map(|i| format!("c{i}"))
        .collect::<Vec<_>>()
        .join(", ");

    let mut inserted_count = 0usize;
    let mut inserted_indices: Vec<usize> = Vec::new();
    let mut rejected: Vec<String> = Vec::new();

    for (idx, (k, cells)) in all_rows.iter().enumerate() {
        let literals: Vec<&str> = cells.iter().map(|c| c.literal.as_str()).collect();
        // The column types are drawn at run time, and the typed DSL needs a schema at compile time.
        let sql = format!(
            "INSERT INTO t (id, {col_list}) VALUES ({k}, {})",
            literals.join(", ")
        );
        match sql_query(&sql).execute(&mut my) {
            Ok(_) => {
                inserted_count += 1;
                inserted_indices.push(idx);
            }
            Err(e) => {
                for cell in cells {
                    if cell.literal != "NULL" {
                        rejected.push(format!("{:?} literal={:?}: {e}", cell.column, cell.literal));
                    }
                }
            }
        }
    }

    let lines = common::maxwell_collect(&out, &db, "t", inserted_count);
    assert_eq!(
        lines.len(),
        inserted_count,
        "Maxwell row count ({}) differs from inserted count ({inserted_count})",
        lines.len()
    );

    let mut my_stats: BTreeMap<&'static str, ColStats> = BTreeMap::new();

    for (line, &row_idx) in lines.iter().zip(inserted_indices.iter()) {
        let mut v: JsonValue = serde_json::from_str(line).expect("Maxwell line is valid JSON");
        let data = v
            .get_mut("data")
            .expect("Maxwell line has a 'data' field")
            .as_object_mut()
            .expect("Maxwell 'data' is a JSON object");
        let (_, cells) = &all_rows[row_idx];

        for (col_idx, cell) in cells.iter().enumerate() {
            let col_ddl = cell.column.ddl();
            let entry = my_stats.entry(col_ddl).or_default();
            entry.compared += 1;

            let col_name = format!("c{col_idx}");
            let actual = data.remove(&col_name).unwrap_or(JsonValue::Null);

            if cell.maxwell != actual {
                entry.mismatches.push((row_idx, col_idx, actual));
            }
        }
    }

    let total: usize = my_stats.values().map(|s| s.mismatches.len()).sum();

    let mut report = format!(
        "=== MySQL Maxwell decoder spelling ===\n\
         Rows attempted: {ROW_COUNT}, generated: {}\n\
         Timezone: UTC (+00:00)\n\n",
        all_rows.len()
    );
    if !rejected.is_empty() {
        let _ = writeln!(report, "Rejected inserts ({}):", rejected.len());
        for r in rejected.iter().take(40) {
            let _ = writeln!(report, "  {r}");
        }
        report.push('\n');
    }
    report.push_str("Column comparison summary:\n");
    for &col in MY_COLUMNS {
        let ddl = col.ddl();
        let default = ColStats::default();
        let stats = my_stats.get(ddl).unwrap_or(&default);
        report.push_str(&fmt_my_col(ddl, stats, &all_rows));
    }
    let _ = writeln!(report, "\nTotal mismatches: {total}");

    assert_eq!(total, 0, "{report}");
}
