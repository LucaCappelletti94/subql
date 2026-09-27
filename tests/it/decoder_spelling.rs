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

/// Every row has to be inserted and every column compared on each row,
/// and every zone has to compare some cell, or the test passed by comparing
/// less than it names.
fn assert_pg_coverage(stats: &PgStats, report: &str) {
    assert!(stats.rejected.is_empty(), "{report}");
    let short: Vec<&str> = PG_COLUMNS
        .iter()
        .filter(|&&col| col != PgColumn::TimestampTz)
        .map(|col| col.ddl())
        .filter(|ddl| stats.columns.get(ddl).map_or(0, |stats| stats.compared) < ROW_COUNT)
        .collect();
    assert!(
        short.is_empty(),
        "columns compared on fewer than {ROW_COUNT} rows: {short:?}\n{report}"
    );
    let silent: Vec<&str> = ZONES
        .iter()
        .map(|&(zone, _, _)| zone)
        .filter(|zone| {
            stats
                .zones
                .get(&(PgColumn::TimestampTz.ddl(), *zone))
                .map_or(0, |stats| stats.compared)
                == 0
        })
        .collect();
    assert!(
        silent.is_empty(),
        "zones that compared no TIMESTAMPTZ cell: {silent:?}\n{report}"
    );
}

/// A session zone, the offset it prints, and whether it prints that offset
/// for every instant.
type Zone = (&'static str, &'static str, bool);

/// One zone per offset the harness draws. The POSIX spellings hold one
/// offset for every instant. PostgreSQL refuses a POSIX offset with seconds,
/// so that one comes from Amsterdam's mean time, which it prints in winter
/// through 1937 and in summer before 1916.
const ZONES: &[Zone] = &[
    ("UTC", "+00", true),
    ("Etc/GMT-1", "+01", true),
    ("Etc/GMT+5", "-05", true),
    ("<+0530>-05:30", "+05:30", true),
    ("<+0545>-05:45", "+05:45", true),
    ("Europe/Amsterdam", "+00:19:32", false),
];

/// What the PostgreSQL passes compared, by column and by zone.
#[derive(Default)]
struct PgStats {
    columns: BTreeMap<&'static str, ColStats>,
    zones: BTreeMap<(&'static str, &'static str), ColStats>,
    rejected: Vec<String>,
}

impl PgStats {
    /// Compare one cell of the pass in `ZONES[zi]`, or count it skipped.
    ///
    /// A cell other than `TIMESTAMPTZ` does not depend on the zone, so it is
    /// compared in the first pass only, as is a `NULL`. A `TIMESTAMPTZ` is
    /// compared in the zone printing its offset, and in a zone with a history
    /// only where PostgreSQL printed that offset.
    fn record(&mut self, zi: usize, at: (usize, usize), cell: &PgCell, actual: JsonValue) {
        let (zone, zone_offset, fixed) = ZONES[zi];
        let ddl = cell.column.ddl();
        let entry = if cell.column == PgColumn::TimestampTz {
            let printed = actual
                .as_str()
                .is_some_and(|printed| tz_matches(printed, zone_offset));
            let entry = self.zones.entry((ddl, zone)).or_default();
            match &cell.text {
                None if zi > 0 => return,
                Some(text) if !tz_matches(text, zone_offset) || (!fixed && !printed) => {
                    entry.tz_skipped += 1;
                    return;
                }
                _ => entry,
            }
        } else if zi > 0 {
            return;
        } else {
            self.columns.entry(ddl).or_default()
        };
        entry.compared += 1;
        if cell.wal2json() != actual {
            entry.mismatches.push((at.0, at.1, actual));
        }
    }

    fn mismatches(&self) -> usize {
        self.columns
            .values()
            .chain(self.zones.values())
            .map(|stats| stats.mismatches.len())
            .sum()
    }
}

/// Insert every row, recording the ones PostgreSQL refuses, and name the
/// rows it took.
fn insert_pg_rows(
    conn: &mut diesel::PgConnection,
    all_rows: &[(usize, Vec<PgCell>)],
    zone: &str,
    rejected: &mut Vec<String>,
) -> Vec<usize> {
    let col_list: String = (0..PG_COLUMNS.len())
        .map(|i| format!("c{i}"))
        .collect::<Vec<_>>()
        .join(", ");
    let mut inserted = Vec::new();
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
        match sql_query(&sql).execute(conn) {
            Ok(_) => inserted.push(idx),
            Err(e) => rejected.extend(cells.iter().filter_map(|cell| {
                cell.text
                    .as_ref()
                    .map(|text| format!("zone={zone} {:?} text={text:?}: {e}", cell.column))
            })),
        }
    }
    inserted
}

/// The columns of every insert into `t` the slot holds, by name.
fn drain_inserts(conn: &mut diesel::PgConnection, slot: &str) -> Vec<BTreeMap<String, JsonValue>> {
    common::drain_slot(conn, slot)
        .iter()
        .filter_map(|line| {
            let v: JsonValue = serde_json::from_str(line).ok()?;
            let obj = v.as_object()?;
            if obj.get("action")?.as_str()? != "I" || obj.get("table")?.as_str()? != "t" {
                return None;
            }
            Some(
                obj.get("columns")?
                    .as_array()?
                    .iter()
                    .filter_map(|c| {
                        let name = c.get("name")?.as_str()?.to_owned();
                        Some((name, c.get("value").cloned().unwrap_or(JsonValue::Null)))
                    })
                    .collect(),
            )
        })
        .collect()
}

fn pg_report(stats: &PgStats, all_rows: &[(usize, Vec<PgCell>)]) -> String {
    let mut report = format!(
        "=== PG wal2json decoder spelling ===\n\
         Rows attempted: {ROW_COUNT}, generated: {}\n\
         Zones: one per offset the harness draws\n\n",
        all_rows.len()
    );
    if !stats.rejected.is_empty() {
        let _ = writeln!(report, "Rejected inserts ({}):", stats.rejected.len());
        for r in stats.rejected.iter().take(40) {
            let _ = writeln!(report, "  {r}");
        }
        report.push('\n');
    }
    report.push_str("Column comparison summary:\n");
    let default = ColStats::default();
    for &col in PG_COLUMNS {
        let ddl = col.ddl();
        if col == PgColumn::TimestampTz {
            for &(zone, zone_offset, _) in ZONES {
                let zone_stats = stats.zones.get(&(ddl, zone)).unwrap_or(&default);
                report.push_str(&fmt_pg_col(
                    &format!("TIMESTAMPTZ [{zone} {zone_offset}]"),
                    zone_stats,
                    all_rows,
                ));
            }
        } else {
            let col_stats = stats.columns.get(ddl).unwrap_or(&default);
            report.push_str(&fmt_pg_col(ddl, col_stats, all_rows));
        }
    }
    let _ = writeln!(report, "\nTotal mismatches: {}", stats.mismatches());
    report
}

#[test]
#[ignore = "requires Docker; run with --ignored"]
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
    let mut stats = PgStats::default();
    for (zi, &(zone, _, _)) in ZONES.iter().enumerate() {
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
        let inserted = insert_pg_rows(&mut conn, &all_rows, zone, &mut stats.rejected);
        let wal_rows = drain_inserts(&mut conn, &slot);
        assert_eq!(
            wal_rows.len(),
            inserted.len(),
            "zone {zone}: wal2json row count differs from inserted count"
        );
        for (mut wal_row, &row_idx) in wal_rows.into_iter().zip(&inserted) {
            for (col_idx, cell) in all_rows[row_idx].1.iter().enumerate() {
                let actual = wal_row
                    .remove(&format!("c{col_idx}"))
                    .unwrap_or(JsonValue::Null);
                stats.record(zi, (row_idx, col_idx), cell, actual);
            }
        }
    }

    let report = pg_report(&stats, &all_rows);
    assert_eq!(stats.mismatches(), 0, "{report}");
    assert_pg_coverage(&stats, &report);
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
    // Every row has to be inserted and every column compared on each row.
    assert!(rejected.is_empty(), "{report}");
    let short: Vec<&str> = MY_COLUMNS
        .iter()
        .map(|col| col.ddl())
        .filter(|ddl| my_stats.get(ddl).map_or(0, |stats| stats.compared) < ROW_COUNT)
        .collect();
    assert!(
        short.is_empty(),
        "columns compared on fewer than {ROW_COUNT} rows: {short:?}\n{report}"
    );
}
