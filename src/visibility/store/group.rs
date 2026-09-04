use alloc::string::{String, ToString};
use alloc::vec::Vec;

use rls2fga_types::{Record, RecordDescription, RelationName, ReplayScope};

/// One producer's unnarrowed enumeration, as the translation reports it.
///
/// Named here rather than taken as `rls2fga`'s own tuple query, because this
/// index is built from `rls2fga-types` alone and the translator is a separate
/// feature. A caller holding a translation maps each of its queries into one of
/// these, dropping the ones that carry no description since those enumerate
/// nothing.
#[derive(Clone, Copy, Debug)]
pub struct Enumeration<'a> {
    /// The producer whose facts these rows are.
    pub description: &'a RecordDescription,
    /// SQL taking no value and carrying no placeholder.
    pub sql: &'a str,
    /// Condition the result rows carry, absent where they carry none.
    pub condition: Option<&'a str>,
}

/// The caller's half of a group reconcile: run one member's query and hand
/// back the records its rows spell.
///
/// subql reaches no database, so the rows can only come from the caller. The
/// loop is driven from this side rather than the caller's because running
/// every member before anything is deleted is the whole correctness argument:
/// a caller free to reconcile one member's rows alone would delete the facts
/// its siblings state in the same region.
pub trait Replayer {
    /// Why a replay could not be run.
    type Error;

    /// Run `member`'s SQL, which takes no value and carries no placeholder,
    /// and return every record it returned.
    ///
    /// Returns a future rather than being an `async fn` so the bound is
    /// stated: a caller driving this from a multi-threaded runtime needs the
    /// future to cross threads.
    fn replay(
        &self,
        member: &Replay,
    ) -> impl core::future::Future<Output = Result<Vec<Record>, Self::Error>> + Send;
}

/// One indivisible piece of a region: the facts of one relation on one object
/// type, either granting to any subject or confined to one subject type.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegionPart {
    object_type: String,
    relation: RelationName,
    subject_type: Option<String>,
}

impl RegionPart {
    /// Type every object in this part belongs to.
    #[must_use]
    pub fn object_type(&self) -> &str {
        &self.object_type
    }

    /// Relation this part covers.
    #[must_use]
    pub const fn relation(&self) -> &RelationName {
        &self.relation
    }

    /// Subject type this part is confined to, absent where it spans every
    /// subject.
    #[must_use]
    pub fn subject_type(&self) -> Option<&str> {
        self.subject_type.as_deref()
    }

    /// Whether the two parts share a fact.
    fn meets(&self, other: &Self) -> bool {
        self.object_type == other.object_type
            && self.relation == other.relation
            && match (self.subject_type.as_ref(), other.subject_type.as_ref()) {
                (Some(one), Some(two)) => one == two,
                // An unconfined part spans every subject, so it meets a
                // confined one on the same relation.
                _ => true,
            }
    }

    /// Whether every fact of `other` is also a fact of this part.
    fn covers(&self, other: &Self) -> bool {
        self.object_type == other.object_type
            && self.relation == other.relation
            && (self.subject_type.is_none() || self.subject_type == other.subject_type)
    }

    /// Whether a fact spelled as the store spells it lies in this part.
    fn holds(&self, object: &str, relation: &str, subject: &str) -> bool {
        typed(object, &self.object_type)
            && self.relation.as_str() == relation
            && self
                .subject_type
                .as_ref()
                .is_none_or(|kind| typed(subject, kind))
    }
}

/// The stored facts one group of producers is authoritative over, together.
///
/// A union of parts rather than one object type with a relation list, because
/// a union of scopes is not always expressible that way: two scopes granting
/// one relation to different subject types would widen to every subject, and
/// the group would then delete facts granting to a third subject type that no
/// member states. Parts keep the union exact, so the region is neither wider
/// nor narrower than the authority its members actually hold.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct Region {
    parts: Vec<RegionPart>,
}

impl Region {
    /// The region one scope declares, unnarrowed.
    #[must_use]
    pub fn of(scope: &ReplayScope) -> Self {
        let parts = match scope {
            ReplayScope::Object {
                object_type,
                relations,
            } => relations
                .iter()
                .map(|relation| RegionPart {
                    object_type: object_type.clone(),
                    relation: relation.clone(),
                    subject_type: None,
                })
                .collect(),
            ReplayScope::Subject {
                subject_type,
                relation,
                object_type,
            } => alloc::vec![RegionPart {
                object_type: object_type.clone(),
                relation: relation.clone(),
                subject_type: Some(subject_type.clone()),
            }],
        };
        let mut region = Self { parts };
        region.normalise();
        region
    }

    /// The region every fact a row-settled producer states falls in.
    ///
    /// Confined to the template's subject type, since every record it makes
    /// carries that type. Broadening it would hand the group authority to
    /// delete facts granting to a type this producer never states, and
    /// grouping is closed over overlap, so the narrow region still gathers
    /// every producer whose facts it could delete.
    #[must_use]
    pub fn of_template(object_type: &str, relation: &RelationName, subject_type: &str) -> Self {
        Self {
            parts: alloc::vec![RegionPart {
                object_type: object_type.to_string(),
                relation: relation.clone(),
                subject_type: Some(subject_type.to_string()),
            }],
        }
    }

    /// Whether the two regions share a fact.
    #[must_use]
    pub fn overlaps(&self, other: &Self) -> bool {
        self.parts
            .iter()
            .any(|part| other.parts.iter().any(|theirs| part.meets(theirs)))
    }

    /// Widen this region to cover `other` as well, exactly.
    pub fn absorb(&mut self, other: &Self) {
        self.parts.extend(other.parts.iter().cloned());
        self.normalise();
    }

    /// Drop every part another part already covers, then order what remains,
    /// so two regions built in different orders compare equal.
    ///
    /// Sorted rather than merely deduplicated because `PartialEq` is derived
    /// and a caller reads it: `materialise` recognises one group named twice in
    /// a pass by comparing regions.
    fn normalise(&mut self) {
        let mut kept: Vec<RegionPart> = Vec::with_capacity(self.parts.len());
        for part in core::mem::take(&mut self.parts) {
            if kept.iter().any(|held| held.covers(&part)) {
                continue;
            }
            kept.retain(|held| !part.covers(held));
            kept.push(part);
        }
        kept.sort_by(|one, two| {
            one.object_type
                .cmp(&two.object_type)
                .then_with(|| one.relation.as_str().cmp(two.relation.as_str()))
                .then_with(|| one.subject_type.cmp(&two.subject_type))
        });
        self.parts = kept;
    }

    /// Whether a fact spelled as the store spells it lies in this region.
    #[must_use]
    pub fn holds(&self, object: &str, relation: &str, subject: &str) -> bool {
        self.parts
            .iter()
            .any(|part| part.holds(object, relation, subject))
    }

    /// Whether `record` lies in this region.
    #[must_use]
    pub fn holds_record(&self, record: &Record) -> bool {
        self.holds(&record.object, record.relation.as_str(), &record.subject)
    }

    /// The parts this region is the union of, normalised.
    #[must_use]
    pub fn parts(&self) -> &[RegionPart] {
        &self.parts
    }
}

/// Whether `id` names a row of `type_name`, which the store spells
/// `type_name:key`.
fn typed(id: &str, type_name: &str) -> bool {
    id.strip_prefix(type_name)
        .is_some_and(|rest| rest.starts_with(':'))
}

/// One producer's unnarrowed query, which returns every fact that producer
/// states.
///
/// Keyless by construction: it takes no value and carries no placeholder, so a
/// caller runs the text as it stands.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Replay {
    sql: String,
    condition: Option<String>,
    region: Region,
}

impl Replay {
    /// Hold `sql` as the enumeration of the facts `region` covers.
    #[must_use]
    pub const fn new(sql: String, condition: Option<String>, region: Region) -> Self {
        Self {
            sql,
            condition,
            region,
        }
    }

    /// SQL projecting object, relation and subject, plus the two further
    /// columns a conditional tuple needs where [`condition`](Self::condition)
    /// is present.
    #[must_use]
    pub fn sql(&self) -> &str {
        &self.sql
    }

    /// Condition the result rows carry, absent where they carry none.
    #[must_use]
    pub fn condition(&self) -> Option<&str> {
        self.condition.as_deref()
    }

    /// The facts this one member determines.
    ///
    /// A region rather than one [`ReplayScope`], because a producer replayable
    /// from several tables declares several scopes and their union is not
    /// always one scope. The region represents the union exactly, so nothing
    /// is discarded and no case is unrepresentable.
    #[must_use]
    pub const fn region(&self) -> &Region {
        &self.region
    }
}

/// Every producer stating facts in one region, reconciled as a single unit.
///
/// The unit is the group rather than the producer because a region's truth is
/// the union of its members: reconciling one member alone would delete the
/// facts its siblings state in the same region. Running every member and
/// unioning the rows is what earns the right to delete.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Materialisation {
    region: Region,
    members: Vec<Replay>,
    constants: Vec<Record>,
}

impl Materialisation {
    /// Hold `members` and `constants` as the whole truth for `region`.
    #[must_use]
    pub const fn new(region: Region, members: Vec<Replay>, constants: Vec<Record>) -> Self {
        Self {
            region,
            members,
            constants,
        }
    }

    /// The facts this group is authoritative over.
    #[must_use]
    pub const fn region(&self) -> &Region {
        &self.region
    }

    /// The queries to run, all of them, before anything is deleted.
    #[must_use]
    pub fn members(&self) -> &[Replay] {
        &self.members
    }

    /// Facts stated with no query behind them, which belong to the union just
    /// as a replayed row does.
    ///
    /// A translation states these outright rather than deriving them from a
    /// row, so no event moves them and no query returns them. Leaving them out
    /// of the union would delete them on the first reconcile.
    #[must_use]
    pub fn constants(&self) -> &[Record] {
        &self.constants
    }
}
