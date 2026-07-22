//! Property: for any catalog, the reexec wrapper accepts an aggregate
//! registration exactly when the target table has row-level security
//! disabled, independent of the aggregate flavor.
//!
//! This pins the guard against a future aggregate variant that forgets
//! the RLS check: rejection is keyed on `has_row_level_security()`, never
//! on the specific aggregate. "Accepted" means `Ok(_)` (a delta-composable
//! aggregate returns `Registered::Engine`, a captured `MIN`/`MAX` returns
//! `Registered::ReExec`); "rejected" means `Err(AggregatorOnRlsTable)`.

#![allow(clippy::unwrap_used)]

use core::fmt::Write as _;

use proptest::prelude::*;
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::Postgres;
use subql::reexec::{ReExecEngine, Registered};
use subql::testing::TestEvent;
use subql::{DefaultIds, RegisterError, SubscriptionEngine, SubscriptionRequest, TableId};

type Engine = ReExecEngine<TestEvent<Postgres>, DefaultIds, ParserDB>;

/// Aggregate flavors spanning both families. `engine_accepts` records
/// whether a non-RLS acceptance is the delta-composable `Registered::Engine`
/// (in-process family) or the captured `Registered::ReExec` (`MIN`/`MAX`).
struct Flavor {
    keyword: &'static str,
    engine_accepts: bool,
}

const FLAVORS: &[Flavor] = &[
    Flavor {
        keyword: "COUNT(*)",
        engine_accepts: true,
    },
    Flavor {
        keyword: "COUNT(amount)",
        engine_accepts: true,
    },
    Flavor {
        keyword: "SUM(amount)",
        engine_accepts: true,
    },
    Flavor {
        keyword: "AVG(amount)",
        engine_accepts: true,
    },
    Flavor {
        keyword: "VAR_POP(amount)",
        engine_accepts: true,
    },
    Flavor {
        keyword: "VAR_SAMP(amount)",
        engine_accepts: true,
    },
    Flavor {
        keyword: "STDDEV_POP(amount)",
        engine_accepts: true,
    },
    Flavor {
        keyword: "STDDEV_SAMP(amount)",
        engine_accepts: true,
    },
    Flavor {
        keyword: "MIN(amount)",
        engine_accepts: false,
    },
    Flavor {
        keyword: "MAX(amount)",
        engine_accepts: false,
    },
];

/// Build DDL for `rls_flags.len()` tables named `t0..`, enabling RLS on
/// the tables whose flag is set.
fn build_ddl(rls_flags: &[bool]) -> String {
    let mut ddl = String::new();
    for (i, &rls) in rls_flags.iter().enumerate() {
        write!(ddl, "CREATE TABLE t{i} (id INT PRIMARY KEY, amount INT); ").unwrap();
        if rls {
            write!(ddl, "ALTER TABLE t{i} ENABLE ROW LEVEL SECURITY; ").unwrap();
        }
    }
    ddl
}

fn table_id_of(catalog: &ParserDB, name: &str) -> TableId {
    subql::catalog_helpers::table_id(catalog, name).unwrap()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    #[test]
    fn accepted_aggregates_are_exactly_non_rls_tables(
        rls_flags in proptest::collection::vec(any::<bool>(), 1..=6),
    ) {
        let ddl = build_ddl(&rls_flags);
        let catalog = ParserDB::parse::<PostgreSqlDialect>(&ddl).unwrap();

        // Expected table ids captured before the engine consumes the catalog.
        let expected: Vec<TableId> = (0..rls_flags.len())
            .map(|i| table_id_of(&catalog, &format!("t{i}")))
            .collect();

        let mut engine: Engine =
            ReExecEngine::new(SubscriptionEngine::new(catalog, PostgreSqlDialect {}));

        let mut consumer_id = 1u64;
        for (i, &rls) in rls_flags.iter().enumerate() {
            for flavor in FLAVORS {
                let sql = format!("SELECT {} FROM t{i}", flavor.keyword);
                let registered = engine.register(
                    SubscriptionRequest::<DefaultIds, Postgres>::new(consumer_id, &sql),
                );
                consumer_id += 1;

                if rls {
                    match registered {
                        Err(RegisterError::AggregatorOnRlsTable { table_id }) => {
                            prop_assert_eq!(
                                table_id, expected[i],
                                "`{}` rejected for the wrong table id", sql
                            );
                        }
                        other => prop_assert!(
                            false,
                            "`{}` on RLS table must be rejected, got {:?}", sql, other
                        ),
                    }
                } else {
                    match registered {
                        Ok(Registered::Engine(result)) => {
                            prop_assert!(
                                flavor.engine_accepts,
                                "`{}` unexpectedly took the engine path", sql
                            );
                            prop_assert!(
                                result.aggregate_spec().is_some(),
                                "`{}` should carry an aggregate spec", sql
                            );
                        }
                        Ok(Registered::ReExec { .. }) => {
                            prop_assert!(
                                !flavor.engine_accepts,
                                "`{}` unexpectedly took the reexec path", sql
                            );
                        }
                        other => prop_assert!(
                            false,
                            "`{}` without RLS must be accepted, got {:?}", sql, other
                        ),
                    }
                }
            }
        }
    }
}
