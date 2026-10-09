//! Shapes over a translated schema with two membership sources pooled onto one relation.

use alloc::string::String;
use alloc::vec::Vec;

use rls2fga::translator::{Translation, TranslatorBuilder};
use rls2fga_types::{ConditionName, ConfidenceLevel, RecordDescription};
use sqlparser::dialect::PostgreSqlDialect;

use crate::backend::Postgres;
use crate::testing::relation_pool::RelationPool;
use crate::visibility::shapes::Shapes;
use crate::visibility::store::Enumeration;
use crate::ParserDB;

/// Shapes over `sql` with the relation `from`'s rows feed pooled into the one `onto`'s rows feed.
///
/// `finish` adds whatever else of the translation the caller's own builder adds.
pub fn shapes(
    sql: &str,
    from: &str,
    onto: &str,
    finish: impl FnOnce(Shapes<ParserDB>, &Translation) -> Shapes<ParserDB>,
) -> Shapes<ParserDB> {
    let db = ParserDB::parse::<PostgreSqlDialect>(sql).expect("the fixture parses");
    let outputs = TranslatorBuilder::new()
        .with_min_confidence(ConfidenceLevel::B)
        .build()
        .translate(&db)
        .expect("the fixture translates")
        .outputs_accepting_gaps();
    let translation = outputs.translation();
    let pool = RelationPool::between(translation.relations(), from, onto);
    let queries: Vec<(RecordDescription, String, Option<&str>)> = outputs
        .tuple_queries()
        .iter()
        .filter_map(|query| {
            query.description.as_ref().map(|description| {
                (
                    pool.description(description),
                    pool.sql(&query.sql),
                    query.condition.as_ref().map(ConditionName::as_str),
                )
            })
        })
        .collect();
    let enumerations: Vec<Enumeration<'_>> = queries
        .iter()
        .map(|(description, sql, condition)| Enumeration {
            description,
            sql,
            condition: *condition,
        })
        .collect();
    finish(
        Shapes::new::<Postgres>(db, &pool.relations(translation.relations()), &enumerations),
        translation,
    )
}
