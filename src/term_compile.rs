//! Asking whether a membership term can be served, and by which table's rows.
//!
//! One filter has two executors: the SQL it was written as, which the snapshot
//! runs, and the records compiled here, which answer per changed row. A filter
//! they answer differently is worse than one that is refused, so this refuses in
//! `rls2fga`'s own wording rather than serving half of it.
//!
//! Switched by the `membership-term` feature, because it is the only half of the
//! term that names an `rls2fga` type. The bounded form itself is recognised in
//! every build, both wasm targets included.

use alloc::format;
use alloc::string::ToString;

use rls2fga::generator::records::{Guard, RecordDerivation, ValueSource};
use rls2fga::generator::relations::RelationShapes;
use rls2fga::term::{describe_membership_term, TermChain, TermShapes};
use rls2fga::translator::Translator;
use sql_traits::prelude::DatabaseLike;

use crate::term::{CompiledTerm, TermMovement, TermPlan};
use crate::{catalog_helpers, RegisterError, TableId};

/// Settle whether `term` on `table` can be served, and how its subscriber set
/// moves.
///
/// # Errors
///
/// [`RegisterError::MembershipTermRefused`] carrying `rls2fga`'s reason when the
/// relationship cannot be compiled, or subql's own reason when it can be but the
/// shape describing it reads something a changed row cannot answer.
pub fn plan_term<B: crate::backend::Backend, DB: DatabaseLike>(
    term: &CompiledTerm,
    table: TableId,
    table_name: &str,
    database: &DB,
    translator: &Translator,
) -> Result<TermPlan, RegisterError> {
    // The effective registry, not the base one: a filter naming a function
    // defined in the schema is accepted inside a real read rule and would be
    // refused here on the base registry alone.
    let (_, registry) = translator.classify_with_effective_registry(database);
    let shapes = describe_membership_term(
        &term.expr,
        database,
        &registry,
        table_name,
        translator.min_confidence(),
    )
    .map_err(|refusal| RegisterError::MembershipTermRefused(refusal.reason))?;

    if term.compares_the_caller() {
        return caller_plan(term, table, database, &shapes);
    }

    let movement = member_columns(&shapes, database)?;

    // The value the term compares has to be the value the membership row keys
    // its object on, or the lookup stores under one key and reads under another.
    // The two coincide by construction for the chain `rls2fga` reports, and this
    // says so rather than trusting it, since a mismatch admits nobody in
    // silence.
    let compared = catalog_helpers::column_scalar_kind::<B, DB>(database, table, term.column);
    let keyed = catalog_helpers::column_scalar_kind::<B, DB>(
        database,
        movement.member_table,
        movement.member_key,
    );
    if compared != keyed {
        return Err(RegisterError::MembershipTermRefused(format!(
            "the column this filter compares and the column the relationship is keyed on hold \
             different kinds of value ({compared:?} against {keyed:?}), so a row's value could \
             never be found in the set the relationship admits"
        )));
    }

    Ok(TermPlan {
        slot: term.slot,
        column: term.column,
        moved_by: Some(movement),
    })
}

/// Settle a caller comparison: the compiled rule has to be the one link naming
/// the caller from the compared column of the subscribed row, with nothing
/// beside it, or the SQL and the term would answer differently.
///
/// Verified against the shapes rather than trusted, in the same spirit as the
/// kind check above: each divergence admits the wrong rows in silence.
fn caller_plan<DB: DatabaseLike>(
    term: &CompiledTerm,
    table: TableId,
    database: &DB,
    shapes: &TermShapes,
) -> Result<TermPlan, RegisterError> {
    let TermChain::Direct { relation } = &shapes.chain else {
        return Err(RegisterError::MembershipTermRefused(
            "this comparison reaches the subscriber through more than the compared row itself, \
             so the subscriber the request states is not the one value it admits"
                .to_string(),
        ));
    };
    let entry = shapes
        .relations
        .iter()
        .find(|entry| &entry.relation == relation)
        .ok_or_else(|| {
            RegisterError::MembershipTermRefused(format!(
                "nothing states the records for '{relation}', so the comparison admits nobody"
            ))
        })?;

    let refuse = |what: &str| {
        RegisterError::MembershipTermRefused(format!(
            "the records for '{relation}' {what}, so the subscriber the request states is not \
             the one value this comparison admits",
            relation = entry.relation,
        ))
    };
    let [shape] = entry.shapes.as_slice() else {
        return Err(refuse("are stated by more than one query, or by none"));
    };
    let RecordDerivation::FromRow {
        table: row_table,
        template,
        guards,
        ..
    } = &shape.derivation
    else {
        return Err(refuse("need more than one row"));
    };
    // A conditional record grants nobody on its own: the service completes the
    // comparison with what the request supplies, and the term performs no such
    // completion.
    if template.context.is_some() {
        return Err(refuse(
            "carry a condition the compared row alone does not answer",
        ));
    }
    // The one guard the term itself enforces: a NULL compared cell keys nothing
    // and admits nobody, which is exactly `NotNull` on the compared column. Any
    // other guard admits records only for rows the term never re-checks.
    let enforced_by_the_lookup = |guard: &Guard| {
        matches!(guard, Guard::NotNull(column)
            if catalog_helpers::column_id(database, table, column.as_str()) == Some(term.column))
    };
    if !guards.iter().all(enforced_by_the_lookup) {
        return Err(refuse(
            "are guarded on more than the compared value being present",
        ));
    }
    if catalog_helpers::table_id(database, row_table) != Some(table) {
        return Err(refuse("read a table other than the subscribed one"));
    }
    if column_of(template.subject_key.part(), database, table) != Some(term.column) {
        return Err(refuse(
            "name the caller from something other than the compared column",
        ));
    }

    Ok(TermPlan {
        slot: term.slot,
        column: term.column,
        moved_by: None,
    })
}

/// The table and the two columns of the shape that names a caller.
///
/// One walk for both chains. `Direct` names the caller from a row of the
/// subscribed table, `Through` from a row of the membership table, and in both
/// cases the shape to read is the one filling the relation whose records name a
/// caller.
fn member_columns<DB: DatabaseLike>(
    shapes: &TermShapes,
    database: &DB,
) -> Result<TermMovement, RegisterError> {
    let relation =
        match &shapes.chain {
            TermChain::Direct { relation } => relation,
            TermChain::Through { member, .. } => member,
            // `TermChain` is `#[non_exhaustive]`: a longer chain is refused rather
            // than read as one of these two, since a middle link nothing tracks
            // leaves the set moving on rows subql never looks at.
            _ => return Err(RegisterError::MembershipTermRefused(
                "this relationship reaches the subscriber by a longer chain than SubQL tracks. \
                 SubQL follows the filtered row to a related row and that row to the \
                 subscriber, and no further."
                    .to_string(),
            )),
        };

    let entry = shapes
        .relations
        .iter()
        .find(|entry| &entry.relation == relation)
        .ok_or_else(|| {
            RegisterError::MembershipTermRefused(format!(
                "nothing states the records for '{relation}', so no changed row moves who this \
                 filter admits"
            ))
        })?;

    read_from_one_row(entry, database)
}

/// Resolve the one shape of `entry` that a changed row settles.
fn read_from_one_row<DB: DatabaseLike>(
    entry: &RelationShapes,
    database: &DB,
) -> Result<TermMovement, RegisterError> {
    let refuse = |what: &str| {
        RegisterError::MembershipTermRefused(format!(
            "the records for '{relation}' {what}, so SubQL cannot tell from a changed row who \
             this filter admits",
            relation = entry.relation,
        ))
    };

    let [shape] = entry.shapes.as_slice() else {
        return Err(refuse("are stated by more than one query, or by none"));
    };
    let RecordDerivation::FromRow {
        table, template, ..
    } = &shape.derivation
    else {
        return Err(refuse("need more than one row"));
    };

    let table_id = catalog_helpers::table_id(database, table)
        .ok_or_else(|| refuse("read a table the catalog does not know"))?;
    let [object_part] = template.object_key.parts() else {
        return Err(refuse("name their object from more than one column"));
    };
    let key = column_of(object_part, database, table_id)
        .ok_or_else(|| refuse("name their object from something other than a column"))?;
    let subject = column_of(template.subject_key.part(), database, table_id)
        .ok_or_else(|| refuse("name their subject from something other than a column"))?;

    Ok(TermMovement {
        member_table: table_id,
        member_key: key,
        member_subject: subject,
    })
}

/// The column `source` reads, when it reads exactly one scalar column of
/// `table`.
///
/// A list column yields one record per element and a JSON path reads inside a
/// value, and neither is a column whose value a row's cell can be looked up by.
/// `ValueSource` is `#[non_exhaustive]`, so a shape a later rls2fga adds falls to
/// the wildcard and refuses rather than being read as a column.
fn column_of<DB: DatabaseLike>(source: &ValueSource, database: &DB, table: TableId) -> Option<u16> {
    match source {
        ValueSource::Column(name) => catalog_helpers::column_id(database, table, name.as_str()),
        _ => None,
    }
}
