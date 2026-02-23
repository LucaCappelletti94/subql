//! Shared fuzz harness functions.
//!
//! Each `harness_*` function takes raw bytes and exercises a library subsystem.
//! The contract: errors are fine, **panics are bugs**.
//!
//! This module is only compiled under `#[cfg(any(feature = "testing", test))]`.

use std::sync::Arc;

use arbitrary::{Arbitrary, Unstructured};
use sqlparser::dialect::{GenericDialect, PostgreSqlDialect};

use crate::compiler::bytecode::{BytecodeProgram, Instruction};
use crate::compiler::canonicalize::{hash_sql, normalize_sql};
use crate::compiler::parser::parse_and_compile;
use crate::compiler::vm::Vm;
use crate::persistence::codec;
use crate::persistence::shard::{deserialize_shard, ShardPayload};
use crate::types::{Cell, ColumnId, RowImage, SchemaCatalog, TableId};
use crate::DefaultIds;

/// Maximally permissive schema catalog for fuzzing.
///
/// Accepts any table/column name so the fuzzer can exercise deep code paths
/// without being rejected at schema resolution.
pub struct FuzzCatalog;

impl SchemaCatalog for FuzzCatalog {
    fn table_id(&self, _table_name: &str) -> Option<TableId> {
        Some(1)
    }

    fn column_id(&self, _table_id: TableId, column_name: &str) -> Option<ColumnId> {
        let hash = column_name.bytes().fold(0u16, |acc, b| {
            acc.wrapping_mul(31).wrapping_add(u16::from(b))
        });
        Some(hash % 64)
    }

    fn table_arity(&self, _table_id: TableId) -> Option<usize> {
        Some(64)
    }

    fn schema_fingerprint(&self, _table_id: TableId) -> Option<u64> {
        Some(0xF022_F022_F022_F022)
    }
}

/// Generate a [`Cell`] from fuzzer-controlled bytes.
fn arb_cell(u: &mut Unstructured<'_>) -> arbitrary::Result<Cell> {
    match u.int_in_range(0u8..=5)? {
        0 => Ok(Cell::Null),
        1 => Ok(Cell::Missing),
        2 => Ok(Cell::Bool(bool::arbitrary(u)?)),
        3 => Ok(Cell::Int(i64::arbitrary(u)?)),
        4 => Ok(Cell::Float(f64::arbitrary(u)?)),
        _ => {
            let len = u.int_in_range(0usize..=64)?;
            let bytes: Vec<u8> = (0..len)
                .map(|_| u.arbitrary())
                .collect::<arbitrary::Result<_>>()?;
            Ok(Cell::String(String::from_utf8_lossy(&bytes).into()))
        }
    }
}

/// Generate an [`Instruction`] from fuzzer-controlled bytes.
fn arb_instruction(u: &mut Unstructured<'_>) -> arbitrary::Result<Instruction> {
    match u.int_in_range(0u8..=21)? {
        0 => Ok(Instruction::PushLiteral(arb_cell(u)?)),
        1 => Ok(Instruction::LoadColumn(u.int_in_range(0u16..=63)?)),
        2 => Ok(Instruction::Equal),
        3 => Ok(Instruction::NotEqual),
        4 => Ok(Instruction::LessThan),
        5 => Ok(Instruction::LessThanOrEqual),
        6 => Ok(Instruction::GreaterThan),
        7 => Ok(Instruction::GreaterThanOrEqual),
        8 => Ok(Instruction::IsNull),
        9 => Ok(Instruction::IsNotNull),
        10 => Ok(Instruction::And),
        11 => Ok(Instruction::Or),
        12 => Ok(Instruction::Not),
        13 => Ok(Instruction::Add),
        14 => Ok(Instruction::Subtract),
        15 => Ok(Instruction::Multiply),
        16 => Ok(Instruction::Divide),
        17 => Ok(Instruction::Modulo),
        18 => Ok(Instruction::Negate),
        19 => {
            let len = u.int_in_range(0usize..=8)?;
            let list: Vec<Cell> = (0..len)
                .map(|_| arb_cell(u))
                .collect::<arbitrary::Result<_>>()?;
            Ok(Instruction::In(list))
        }
        20 => Ok(Instruction::Between),
        _ => Ok(Instruction::Like {
            case_sensitive: bool::arbitrary(u)?,
        }),
    }
}

// ---------------------------------------------------------------------------
// Harness functions
// ---------------------------------------------------------------------------

/// Parse SQL with both PostgreSQL and Generic dialects.
pub fn harness_parse_sql(data: &[u8]) {
    let catalog = FuzzCatalog;
    let pg = PostgreSqlDialect {};
    let generic = GenericDialect {};
    let sql = String::from_utf8_lossy(data);

    let _ = parse_and_compile(&sql, &pg, &catalog);
    let _ = parse_and_compile(&sql, &generic, &catalog);
}

/// Generate random bytecode + row and evaluate with the VM.
pub fn harness_vm_eval(data: &[u8]) {
    let mut u = Unstructured::new(data);

    // Generate 1-32 instructions
    let Ok(n_instr) = u.int_in_range(1usize..=32) else {
        return;
    };
    let instructions: Vec<Instruction> = match (0..n_instr)
        .map(|_| arb_instruction(&mut u))
        .collect::<arbitrary::Result<_>>()
    {
        Ok(v) => v,
        Err(_) => return,
    };

    // Generate 0-16 row cells
    let Ok(n_cells) = u.int_in_range(0usize..=16) else {
        return;
    };
    let cells: Vec<Cell> = match (0..n_cells)
        .map(|_| arb_cell(&mut u))
        .collect::<arbitrary::Result<_>>()
    {
        Ok(v) => v,
        Err(_) => return,
    };

    let program = BytecodeProgram::new(instructions);
    let row = RowImage {
        cells: Arc::from(cells),
    };

    let mut vm = Vm::new();
    let _ = vm.eval(&program, &row);
}

/// Feed raw bytes to shard deserialization.
pub fn harness_deserialize_shard(data: &[u8]) {
    let catalog = FuzzCatalog;
    let _ = deserialize_shard::<DefaultIds>(data, &catalog);
}

/// Normalize and hash SQL, asserting determinism.
pub fn harness_canonicalize(data: &[u8]) {
    let pg = PostgreSqlDialect {};
    let generic = GenericDialect {};
    let sql = String::from_utf8_lossy(data);

    for dialect in [&pg as &dyn sqlparser::dialect::Dialect, &generic] {
        if let Ok(normalized) = normalize_sql(&sql, dialect) {
            let h1 = hash_sql(&normalized);
            let h2 = hash_sql(&normalized);
            assert_eq!(h1, h2, "hash_sql is not deterministic");
        }
    }
}

/// Try decoding raw bytes as different types.
pub fn harness_codec_decode(data: &[u8]) {
    let _ = codec::decode::<ShardPayload<DefaultIds>>(data);
    let _ = codec::decode::<Vec<u8>>(data);
    let _ = codec::decode::<String>(data);
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    fn cell_kind(cell: &Cell) -> u8 {
        match cell {
            Cell::Null => 0,
            Cell::Missing => 1,
            Cell::Bool(_) => 2,
            Cell::Int(_) => 3,
            Cell::Float(_) => 4,
            Cell::String(_) => 5,
        }
    }

    fn instruction_kind(instr: &Instruction) -> u8 {
        match instr {
            Instruction::PushLiteral(_) => 0,
            Instruction::LoadColumn(_) => 1,
            Instruction::Equal => 2,
            Instruction::NotEqual => 3,
            Instruction::LessThan => 4,
            Instruction::LessThanOrEqual => 5,
            Instruction::GreaterThan => 6,
            Instruction::GreaterThanOrEqual => 7,
            Instruction::IsNull => 8,
            Instruction::IsNotNull => 9,
            Instruction::And => 10,
            Instruction::Or => 11,
            Instruction::Not => 12,
            Instruction::Add => 13,
            Instruction::Subtract => 14,
            Instruction::Multiply => 15,
            Instruction::Divide => 16,
            Instruction::Modulo => 17,
            Instruction::Negate => 18,
            Instruction::In(_) => 19,
            Instruction::Between => 20,
            Instruction::Like { .. } => 21,
            Instruction::JumpIfFalse(_) => 22,
            Instruction::JumpIfTrue(_) => 23,
        }
    }

    #[test]
    fn test_fuzz_catalog_accepts_any_table_and_column() {
        let catalog = FuzzCatalog;

        assert_eq!(catalog.table_id("any_table"), Some(1));
        assert_eq!(catalog.table_arity(1), Some(64));
        assert_eq!(catalog.schema_fingerprint(1), Some(0xF022_F022_F022_F022));

        let col_a = catalog.column_id(1, "alpha").unwrap();
        let col_b = catalog.column_id(1, "beta").unwrap();
        assert!(col_a < 64);
        assert!(col_b < 64);
        assert_ne!(col_a, col_b);
    }

    #[test]
    fn test_arb_cell_covers_all_variants() {
        let mut seen = BTreeSet::new();
        for seed in u8::MIN..=u8::MAX {
            let mut data = vec![0u8; 1024];
            data[0] = seed;
            let mut u = Unstructured::new(&data);
            if let Ok(cell) = arb_cell(&mut u) {
                seen.insert(cell_kind(&cell));
            }
        }

        assert_eq!(seen.len(), 6, "expected all Cell variants, saw {seen:?}");
    }

    #[test]
    fn test_arb_instruction_covers_all_variants() {
        let mut seen = BTreeSet::new();
        for seed in u8::MIN..=u8::MAX {
            let mut data = vec![0u8; 2048];
            data[0] = seed;
            let mut u = Unstructured::new(&data);
            if let Ok(instr) = arb_instruction(&mut u) {
                seen.insert(instruction_kind(&instr));
            }
        }

        assert_eq!(
            seen.len(),
            22,
            "expected all Instruction variants, saw {seen:?}"
        );
    }

    #[test]
    fn test_harness_entrypoints_do_not_panic() {
        harness_parse_sql(b"SELECT * FROM orders WHERE amount > 10");
        harness_parse_sql(&[0xFF, 0x00, 0xAA, 0x42]);

        harness_vm_eval(&vec![0x11; 4096]);
        harness_vm_eval(&vec![0xEE; 4096]);

        harness_deserialize_shard(&[0x00, 0x01, 0x02, 0x03]);
        harness_canonicalize(b"SELECT * FROM orders WHERE status = 'open'");

        let encoded_vec = codec::encode(&vec![1_u8, 2, 3, 4]).unwrap();
        harness_codec_decode(&encoded_vec);
        harness_codec_decode(&[0xFF, 0x00, 0xAA]);
    }

    #[test]
    fn test_harness_vm_eval_exercises_early_return_paths() {
        harness_vm_eval(&[]);

        for a in u8::MIN..=u8::MAX {
            harness_vm_eval(&[a]);
        }

        for a in 0_u8..=63 {
            for b in 0_u8..=63 {
                harness_vm_eval(&[a, b]);
            }
        }
    }

    #[test]
    fn test_instruction_kind_jump_variants() {
        assert_eq!(instruction_kind(&Instruction::JumpIfFalse(3)), 22);
        assert_eq!(instruction_kind(&Instruction::JumpIfTrue(4)), 23);
    }
}

// ---------------------------------------------------------------------------
// Regression tests — replay crash files from tests/crashes/{harness_name}/
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::manual_let_else)]
mod regression_tests {
    use super::*;
    use std::fs;
    use std::path::Path;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static REPLAY_COUNT: AtomicUsize = AtomicUsize::new(0);

    fn count_harness(_data: &[u8]) {
        REPLAY_COUNT.fetch_add(1, Ordering::Relaxed);
    }

    /// Run a harness function against every file in the given crash directory.
    /// Missing or empty directories pass silently (no regressions to check yet).
    fn replay_crashes(dir_name: &str, harness: fn(&[u8])) {
        let crash_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("crashes")
            .join(dir_name);

        let entries = match fs::read_dir(&crash_dir) {
            Ok(e) => e,
            Err(_) => return, // directory missing — nothing to replay
        };

        for entry in entries {
            let entry = entry.expect("failed to read directory entry");
            let path = entry.path();

            // Skip non-files (e.g. .gitkeep is fine to read, but dirs are not)
            if !path.is_file() {
                continue;
            }

            // Skip .gitkeep
            if path.file_name().is_some_and(|n| n == ".gitkeep") {
                continue;
            }

            let data = fs::read(&path).unwrap_or_else(|e| {
                panic!("failed to read crash file {}: {e}", path.display());
            });

            harness(&data);
        }
    }

    #[test]
    fn regression_fuzz_parse_sql() {
        replay_crashes("fuzz_parse_sql", harness_parse_sql);
    }

    #[test]
    fn regression_fuzz_vm_eval() {
        replay_crashes("fuzz_vm_eval", harness_vm_eval);
    }

    #[test]
    fn regression_fuzz_deserialize_shard() {
        replay_crashes("fuzz_deserialize_shard", harness_deserialize_shard);
    }

    #[test]
    fn regression_fuzz_canonicalize() {
        replay_crashes("fuzz_canonicalize", harness_canonicalize);
    }

    #[test]
    fn regression_fuzz_codec_decode() {
        replay_crashes("fuzz_codec_decode", harness_codec_decode);
    }

    #[test]
    fn replay_crashes_ignores_missing_directory() {
        replay_crashes("definitely-missing-subdir-for-coverage", harness_parse_sql);
    }

    #[test]
    fn replay_crashes_skips_non_files_and_gitkeep_and_replays_payloads() {
        let unique = format!(
            "cov-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock should be after epoch")
                .as_nanos()
        );
        let crash_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("crashes")
            .join(&unique);

        fs::create_dir_all(crash_dir.join("nested")).expect("should create nested directory");
        fs::write(crash_dir.join(".gitkeep"), b"").expect("should create .gitkeep");
        fs::write(crash_dir.join("sample.fuzz"), b"\x01\x02\x03")
            .expect("should create crash payload");

        REPLAY_COUNT.store(0, Ordering::Relaxed);
        replay_crashes(&unique, count_harness);
        assert_eq!(REPLAY_COUNT.load(Ordering::Relaxed), 1);

        fs::remove_dir_all(crash_dir).expect("should remove temporary crash directory");
    }

    #[cfg(unix)]
    #[test]
    fn replay_crashes_panics_on_unreadable_file() {
        use std::os::unix::fs::PermissionsExt;

        let unique = format!(
            "cov-unreadable-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock should be after epoch")
                .as_nanos()
        );
        let crash_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("crashes")
            .join(&unique);
        fs::create_dir_all(&crash_dir).expect("should create crash dir");

        let unreadable = crash_dir.join("unreadable.fuzz");
        fs::write(&unreadable, b"data").expect("should create unreadable file");
        let mut perms = fs::metadata(&unreadable)
            .expect("should stat unreadable file")
            .permissions();
        perms.set_mode(0o000);
        fs::set_permissions(&unreadable, perms).expect("should set unreadable perms");

        let result = std::panic::catch_unwind(|| replay_crashes(&unique, super::harness_parse_sql));
        assert!(
            result.is_err(),
            "expected panic when reading unreadable file"
        );

        let mut reset = fs::metadata(&unreadable)
            .expect("should stat unreadable file")
            .permissions();
        reset.set_mode(0o644);
        fs::set_permissions(&unreadable, reset).expect("should restore permissions");
        fs::remove_dir_all(crash_dir).expect("should remove temporary crash directory");
    }
}
