//! Shared fuzz harness functions.
//!
//! Each `harness_*` function takes raw bytes and exercises a library subsystem.
//! The contract: errors are fine, **panics are bugs**.
//!
//! This module is only compiled under `#[cfg(feature = "testing")]`.

// Clippy allows scoped to this fuzz-harness module. These lints flag
// stylistic patterns (manual let-else, items after statements, doc
// paragraph length, identical match arms, by-value generated test
// data, and `BTreeMap` contains_key+insert) that are intentional or
// load-bearing for readability in arbitrary-driven test code. The
// module is feature-gated behind `testing` and is not part of the
// production lib build.
#![allow(
    clippy::manual_let_else,
    clippy::too_long_first_doc_paragraph,
    clippy::items_after_statements,
    clippy::needless_pass_by_value,
    clippy::map_entry,
    clippy::match_same_arms
)]

pub(crate) mod aggregate_consistency;
pub(crate) mod harness_functions;
pub(crate) mod snapshot_restore;

pub use aggregate_consistency::harness_aggregate_consistency;
pub use harness_functions::{
    fuzz_catalog, harness_canonicalize, harness_codec_decode, harness_deserialize_shard,
    harness_parse_sql, harness_vm_eval, harness_wal_json_postparse,
};
#[cfg(feature = "pg-sqlite-emu")]
pub use snapshot_restore::harness_sqlite_pgoutput_e2e;
pub use snapshot_restore::{harness_pgoutput, harness_snapshot_restore_roundtrip};

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::harness_functions::{arb_instruction, arb_value};
    use super::{
        fuzz_catalog, harness_canonicalize, harness_codec_decode, harness_deserialize_shard,
        harness_parse_sql, harness_vm_eval,
    };
    use crate::backend::{Postgres, Value};
    use crate::compiler::bytecode::Instruction;
    use crate::persistence::codec;
    use arbitrary::Unstructured;
    use std::collections::BTreeSet;

    fn value_kind(v: &Value<Postgres>) -> u8 {
        match v {
            Value::Null => 0,
            Value::Missing => 1,
            Value::Bool(_) => 2,
            Value::Int(_) => 3,
            Value::Float(_) => 4,
            Value::String(_) => 5,
            // arb_value never emits these variants; keep exhaustive.
            _ => 99,
        }
    }

    fn instruction_kind(instr: &Instruction<Postgres>) -> u8 {
        match instr {
            Instruction::PushLiteral(_) => 0,
            Instruction::LoadColumn(_) => 1,
            Instruction::Equal(_) => 2,
            Instruction::NotEqual(_) => 3,
            Instruction::LessThan(_) => 4,
            Instruction::LessThanOrEqual(_) => 5,
            Instruction::GreaterThan(_) => 6,
            Instruction::GreaterThanOrEqual(_) => 7,
            Instruction::IsNull => 8,
            Instruction::IsNotNull => 9,
            Instruction::And => 10,
            Instruction::Or => 11,
            Instruction::Not => 12,
            Instruction::Add(_) => 13,
            Instruction::Subtract(_) => 14,
            Instruction::Multiply(_) => 15,
            Instruction::Divide(..) => 16,
            Instruction::Modulo(_) => 17,
            Instruction::Negate(_) => 18,
            Instruction::In { .. } => 19,
            Instruction::Between { .. } => 20,
            Instruction::Like { .. } => 21,
            Instruction::JumpIfFalse(_) => 22,
            Instruction::JumpIfTrue(_) => 23,
            Instruction::TermTruth(_) => 24,
        }
    }

    #[test]
    fn test_fuzz_catalog_resolves_orders_fixture() {
        use sql_traits::prelude::DatabaseLike;

        let catalog = fuzz_catalog();
        let tid = crate::catalog_helpers::table_id(&catalog, "orders")
            .expect("orders must be resolvable in fuzz fixture");
        assert!(catalog.number_of_tables() > 0);
        let arity = crate::catalog_helpers::table_arity(&catalog, tid)
            .expect("orders arity should be known");
        assert!(
            arity >= 3,
            "fuzz orders should have at least id/amount/status"
        );

        let id_col = crate::catalog_helpers::column_id(&catalog, tid, "id");
        let amount_col = crate::catalog_helpers::column_id(&catalog, tid, "amount");
        assert!(id_col.is_some());
        assert!(amount_col.is_some());
        assert_ne!(id_col, amount_col);
    }

    #[test]
    fn test_arb_value_covers_all_generated_variants() {
        let mut seen = BTreeSet::new();
        for seed in u8::MIN..=u8::MAX {
            let mut data = vec![0u8; 1024];
            data[0] = seed;
            let mut u = Unstructured::new(&data);
            if let Ok(v) = arb_value(&mut u) {
                seen.insert(value_kind(&v));
            }
        }

        assert_eq!(
            seen.len(),
            6,
            "expected all 6 arb_value shapes, saw {seen:?}"
        );
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
            24,
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

// Regression tests: replay crash files from tests/crashes/{harness_name}/

#[cfg(test)]
#[allow(clippy::manual_let_else)]
mod regression_tests {
    use super::{
        harness_aggregate_consistency, harness_canonicalize, harness_codec_decode,
        harness_deserialize_shard, harness_parse_sql, harness_pgoutput,
        harness_snapshot_restore_roundtrip, harness_vm_eval, harness_wal_json_postparse,
    };
    use core::sync::atomic::{AtomicUsize, Ordering};
    use std::fs;
    use std::path::Path;

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
            Err(_) => return, // directory missing, nothing to replay
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
    fn regression_fuzz_pgoutput() {
        replay_crashes("fuzz_pgoutput", harness_pgoutput);
    }

    #[test]
    fn regression_fuzz_wal_json_postparse() {
        replay_crashes("fuzz_wal_json_postparse", harness_wal_json_postparse);
    }

    #[test]
    fn regression_fuzz_aggregate_consistency() {
        replay_crashes("fuzz_aggregate_consistency", harness_aggregate_consistency);
    }

    #[test]
    fn regression_fuzz_snapshot_restore_roundtrip() {
        replay_crashes(
            "fuzz_snapshot_restore_roundtrip",
            harness_snapshot_restore_roundtrip,
        );
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

        let result = std::panic::catch_unwind(|| replay_crashes(&unique, harness_parse_sql));
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
