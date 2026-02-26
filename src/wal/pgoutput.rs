//! pgoutput binary replication protocol parser.
//!
//! Parses PostgreSQL's native logical replication wire format directly,
//! without depending on external crates like `pg_walstream`.
//!
//! Unlike the wal2json parsers (stateless unit structs), pgoutput is
//! **stateful**: the protocol sends Relation (`'R'`) messages that define
//! table schemas before DML messages reference them by OID. The parser
//! caches these in a `Mutex<AHashMap<u32, CachedRelation>>`.

use std::sync::{Arc, Mutex};

use ahash::AHashMap;
use std::collections::{HashSet, VecDeque};

use super::pg_type::text_to_cell_strict;
use super::{
    build_pk_from_resolved_strict, delete_event, insert_event, pk_from_catalog_or_empty,
    resolve_table, truncate_event, update_event_with_old_row_completeness, WalParseError,
    WalParser,
};
#[cfg(test)]
use crate::EventKind;
use crate::{Cell, ColumnId, PrimaryKey, RowImage, SchemaCatalog, TableId, WalEvent};

/// Defensive bound to prevent pathological allocations from malformed input.
const MAX_COLUMNS_PER_MESSAGE: usize = 10_000;
#[cfg(not(test))]
const MAX_CACHED_RELATIONS: usize = 65_536;
#[cfg(test)]
const MAX_CACHED_RELATIONS: usize = 32;

// ============================================================================
// Cached relation metadata
// ============================================================================

#[derive(Clone, Debug)]
struct CachedColumn {
    name: String,
    type_oid: u32,
    flags: u8, // bit 0 = part of replica identity key
}

#[derive(Clone, Debug)]
struct CachedRelation {
    #[allow(dead_code)]
    namespace: String,
    #[allow(dead_code)]
    name: String,
    columns: Vec<CachedColumn>,
    table_id: TableId,
    column_ids: Vec<ColumnId>,
    arity: usize,
    /// Indices into `columns` where `flags & 1 != 0` (replica identity).
    identity_columns: Vec<usize>,
}

#[derive(Default)]
struct RelationCache {
    map: AHashMap<u32, CachedRelation>,
    insertion_order: VecDeque<u32>,
}

impl RelationCache {
    fn insert(&mut self, oid: u32, relation: CachedRelation) {
        if self.map.contains_key(&oid) {
            self.map.insert(oid, relation);
            if let Some(pos) = self
                .insertion_order
                .iter()
                .position(|existing| *existing == oid)
            {
                let _ = self.insertion_order.remove(pos);
            }
            self.insertion_order.push_back(oid);
            return;
        }

        if self.map.len() >= MAX_CACHED_RELATIONS {
            if let Some(oldest_oid) = self.insertion_order.pop_front() {
                self.map.remove(&oldest_oid);
            }
        }

        self.map.insert(oid, relation);
        self.insertion_order.push_back(oid);
    }
}

// ============================================================================
// Binary cursor — zero-copy, safe, no external deps
// ============================================================================

struct Cursor<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    const fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    const fn remaining(&self) -> usize {
        self.data.len() - self.pos
    }

    fn check(&self, n: usize) -> Result<(), WalParseError> {
        let expected = self.pos.checked_add(n).ok_or_else(|| {
            WalParseError::MalformedPayload("binary cursor length overflow".to_string())
        })?;
        if self.remaining() < n {
            return Err(WalParseError::TruncatedMessage {
                expected,
                actual: self.data.len(),
            });
        }
        Ok(())
    }

    fn read_u8(&mut self) -> Result<u8, WalParseError> {
        self.check(1)?;
        let v = self.data[self.pos];
        self.pos += 1;
        Ok(v)
    }

    fn read_i16(&mut self) -> Result<i16, WalParseError> {
        self.check(2)?;
        let v = i16::from_be_bytes([self.data[self.pos], self.data[self.pos + 1]]);
        self.pos += 2;
        Ok(v)
    }

    fn read_i32(&mut self) -> Result<i32, WalParseError> {
        self.check(4)?;
        let v = i32::from_be_bytes([
            self.data[self.pos],
            self.data[self.pos + 1],
            self.data[self.pos + 2],
            self.data[self.pos + 3],
        ]);
        self.pos += 4;
        Ok(v)
    }

    fn read_u32(&mut self) -> Result<u32, WalParseError> {
        self.check(4)?;
        let v = u32::from_be_bytes([
            self.data[self.pos],
            self.data[self.pos + 1],
            self.data[self.pos + 2],
            self.data[self.pos + 3],
        ]);
        self.pos += 4;
        Ok(v)
    }

    #[allow(dead_code)]
    fn read_i64(&mut self) -> Result<i64, WalParseError> {
        self.check(8)?;
        let v = i64::from_be_bytes([
            self.data[self.pos],
            self.data[self.pos + 1],
            self.data[self.pos + 2],
            self.data[self.pos + 3],
            self.data[self.pos + 4],
            self.data[self.pos + 5],
            self.data[self.pos + 6],
            self.data[self.pos + 7],
        ]);
        self.pos += 8;
        Ok(v)
    }

    /// Read a NUL-terminated C string, returning the bytes before the NUL as UTF-8.
    fn read_cstring(&mut self) -> Result<&'a str, WalParseError> {
        let start = self.pos;
        while self.pos < self.data.len() {
            if self.data[self.pos] == 0 {
                let s = std::str::from_utf8(&self.data[start..self.pos])
                    .map_err(|e| WalParseError::InvalidUtf8(e.to_string()))?;
                self.pos += 1; // skip NUL
                return Ok(s);
            }
            self.pos += 1;
        }
        Err(WalParseError::TruncatedMessage {
            expected: self.pos + 1,
            actual: self.data.len(),
        })
    }

    fn read_bytes(&mut self, n: usize) -> Result<&'a [u8], WalParseError> {
        self.check(n)?;
        let s = &self.data[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }

    #[allow(dead_code)]
    fn skip(&mut self, n: usize) -> Result<(), WalParseError> {
        self.check(n)?;
        self.pos += n;
        Ok(())
    }
}

// ============================================================================
// Parser
// ============================================================================

/// Parser for PostgreSQL's native pgoutput binary replication protocol.
///
/// Stateful: caches Relation messages so that subsequent DML messages can
/// reference table schemas by OID.
pub struct PgOutputParser {
    relations: Mutex<RelationCache>,
}

impl Default for PgOutputParser {
    fn default() -> Self {
        Self {
            relations: Mutex::new(RelationCache::default()),
        }
    }
}

impl PgOutputParser {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn parse_bounded_count(
        count: i64,
        max_allowed: usize,
        context: &str,
        count_kind: &str,
    ) -> Result<usize, WalParseError> {
        if count < 0 {
            return Err(WalParseError::MalformedPayload(format!(
                "negative {count_kind} count in {context}: {count}"
            )));
        }
        let count = usize::try_from(count).map_err(|_| {
            WalParseError::MalformedPayload(format!(
                "invalid {count_kind} count in {context}: {count}"
            ))
        })?;
        if count > max_allowed {
            return Err(WalParseError::MalformedPayload(format!(
                "{count_kind} count too large in {context}: {count}"
            )));
        }
        Ok(count)
    }

    fn parse_column_count(count: i16, context: &str) -> Result<usize, WalParseError> {
        Self::parse_bounded_count(i64::from(count), MAX_COLUMNS_PER_MESSAGE, context, "column")
    }

    fn parse_relation_count(count: i32, context: &str) -> Result<usize, WalParseError> {
        Self::parse_bounded_count(i64::from(count), MAX_CACHED_RELATIONS, context, "relation")
    }

    fn expected_tag(tag: u8, expected: u8, context: &str) -> Result<(), WalParseError> {
        if tag == expected {
            Ok(())
        } else {
            Err(WalParseError::MalformedPayload(format!(
                "invalid {context} tag: expected 0x{expected:02X}, got 0x{tag:02X}"
            )))
        }
    }

    fn ensure_fully_consumed(cur: &Cursor<'_>, context: &str) -> Result<(), WalParseError> {
        let trailing = cur.remaining();
        if trailing == 0 {
            return Ok(());
        }
        Err(WalParseError::MalformedPayload(format!(
            "trailing bytes after {context}: {trailing}"
        )))
    }

    /// Handle Relation message: parse schema and cache for later DML lookups.
    fn handle_relation(
        &self,
        cur: &mut Cursor<'_>,
        catalog: &dyn SchemaCatalog,
    ) -> Result<(), WalParseError> {
        let oid = cur.read_u32()?;
        let namespace = cur.read_cstring()?.to_string();
        let name = cur.read_cstring()?.to_string();
        let _replica_identity = cur.read_u8()?;
        let num_columns = Self::parse_column_count(cur.read_i16()?, "Relation")?;

        let mut columns = Vec::with_capacity(num_columns);
        for _ in 0..num_columns {
            let flags = cur.read_u8()?;
            let col_name = cur.read_cstring()?.to_string();
            let type_oid = cur.read_u32()?;
            let _type_modifier = cur.read_i32()?;
            columns.push(CachedColumn {
                name: col_name,
                type_oid,
                flags,
            });
        }

        let table_id = resolve_table(&namespace, &name, catalog)?;
        let arity = catalog
            .table_arity(table_id)
            .ok_or_else(|| WalParseError::UnknownTable {
                schema: namespace.clone(),
                table: name.clone(),
            })?;

        let mut column_ids = Vec::with_capacity(columns.len());
        let mut seen_column_ids = HashSet::with_capacity(columns.len());
        for col in &columns {
            let col_id = catalog.column_id(table_id, &col.name).ok_or_else(|| {
                WalParseError::UnknownColumn {
                    table_id,
                    column: col.name.clone(),
                }
            })?;
            if !seen_column_ids.insert(col_id) {
                return Err(WalParseError::MalformedPayload(format!(
                    "relation '{}' column '{}' resolves to duplicate column id {} for table {}",
                    name, col.name, col_id, table_id
                )));
            }
            if (col_id as usize) >= arity {
                return Err(WalParseError::MalformedPayload(format!(
                    "relation column '{}' resolved to out-of-range column id {} for table {} (arity {})",
                    col.name, col_id, table_id, arity
                )));
            }
            column_ids.push(col_id);
        }

        let identity_columns: Vec<usize> = columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.flags & 1 != 0)
            .map(|(i, _)| i)
            .collect();

        let cached = CachedRelation {
            namespace,
            name,
            columns,
            table_id,
            column_ids,
            arity,
            identity_columns,
        };

        // Lock, insert, drop lock immediately.
        {
            let mut map = self
                .relations
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            map.insert(oid, cached);
        }

        Ok(())
    }

    /// Look up a cached relation by OID. Clones it out of the Mutex so the
    /// lock is held only briefly.
    fn get_relation(&self, oid: u32) -> Result<CachedRelation, WalParseError> {
        let map = self
            .relations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        map.map
            .get(&oid)
            .cloned()
            .ok_or(WalParseError::UnknownRelationOid(oid))
    }

    /// Parse a TupleData section, returning a `RowImage` and resolved pairs.
    fn parse_tuple_data_with_positions(
        cur: &mut Cursor<'_>,
        rel: &CachedRelation,
        positions: &[usize],
        context: &str,
        is_new_tuple: bool,
    ) -> Result<(RowImage, Vec<(ColumnId, Cell)>), WalParseError> {
        if positions.len() > rel.columns.len() || positions.len() > rel.column_ids.len() {
            return Err(WalParseError::MalformedPayload(format!(
                "{context} column count {} exceeds relation column count {}",
                positions.len(),
                rel.columns.len()
            )));
        }

        let mut cells = vec![Cell::Missing; rel.arity];
        let mut resolved = Vec::with_capacity(positions.len());

        for &rel_idx in positions {
            if rel_idx >= rel.columns.len() || rel_idx >= rel.column_ids.len() {
                return Err(WalParseError::MalformedPayload(format!(
                    "{context} column index {rel_idx} is out of bounds for relation column count {}",
                    rel.columns.len()
                )));
            }
            let cell = Self::parse_tuple_cell(cur, rel.columns[rel_idx].type_oid, is_new_tuple)?;
            let col_id = rel.column_ids[rel_idx];
            if (col_id as usize) < rel.arity {
                cells[col_id as usize] = cell.clone();
            }
            resolved.push((col_id, cell));
        }

        Ok((
            RowImage {
                cells: Arc::from(cells),
            },
            resolved,
        ))
    }

    fn parse_tuple_data(
        cur: &mut Cursor<'_>,
        rel: &CachedRelation,
        is_new_tuple: bool,
    ) -> Result<(RowImage, Vec<(ColumnId, Cell)>), WalParseError> {
        let num_columns = Self::parse_column_count(cur.read_i16()?, "TupleData")?;
        if num_columns != rel.arity {
            return Err(WalParseError::ArityMismatch {
                table_id: rel.table_id,
                wal_count: num_columns,
                catalog_arity: rel.arity,
            });
        }
        let positions: Vec<usize> = (0..num_columns).collect();
        Self::parse_tuple_data_with_positions(cur, rel, &positions, "tuple", is_new_tuple)
    }

    fn parse_tuple_cell(
        cur: &mut Cursor<'_>,
        type_oid: u32,
        is_new_tuple: bool,
    ) -> Result<Cell, WalParseError> {
        let tag = cur.read_u8()?;
        match tag {
            b'n' => Ok(Cell::Null),
            b'u' if is_new_tuple => Err(WalParseError::MalformedPayload(
                "unchanged-TOAST tag 'u' is not valid in a new-image tuple".to_string(),
            )),
            b'u' => Ok(Cell::Missing),
            b't' => {
                let len_i32 = cur.read_i32()?;
                if len_i32 < 0 {
                    return Err(WalParseError::MalformedPayload(format!(
                        "negative tuple text length: {len_i32}"
                    )));
                }
                let len = usize::try_from(len_i32).map_err(|_| {
                    WalParseError::MalformedPayload(format!("invalid tuple text length: {len_i32}"))
                })?;
                let bytes = cur.read_bytes(len)?;
                let text = std::str::from_utf8(bytes)
                    .map_err(|e| WalParseError::InvalidUtf8(e.to_string()))?;
                text_to_cell_strict(text, type_oid)
            }
            other => Err(WalParseError::UnknownTupleTag(other)),
        }
    }

    /// Parse an UPDATE/DELETE old-key tuple (`'K'`): supports both full-width
    /// tuple encoding and compact key-only encoding mapped by replica identity.
    fn parse_key_tuple_data(
        cur: &mut Cursor<'_>,
        rel: &CachedRelation,
    ) -> Result<(RowImage, Vec<(ColumnId, Cell)>), WalParseError> {
        let num_columns = Self::parse_column_count(cur.read_i16()?, "TupleData")?;
        if num_columns > rel.columns.len() || num_columns > rel.column_ids.len() {
            return Err(WalParseError::MalformedPayload(format!(
                "key tuple column count {} exceeds relation column count {}",
                num_columns,
                rel.columns.len()
            )));
        }

        let mapped_positions: Vec<usize> = if !rel.identity_columns.is_empty()
            && num_columns == rel.identity_columns.len()
        {
            rel.identity_columns.clone()
        } else if num_columns == rel.columns.len() {
            (0..num_columns).collect()
        } else {
            return Err(WalParseError::MalformedPayload(format!(
                "key tuple column count {num_columns} does not match identity column count {} or relation column count {}",
                rel.identity_columns.len(),
                rel.columns.len()
            )));
        };

        Self::parse_tuple_data_with_positions(cur, rel, &mapped_positions, "key tuple", false)
    }

    fn pk_from_old_resolved(
        rel: &CachedRelation,
        old_resolved: &[(ColumnId, Cell)],
    ) -> Result<PrimaryKey, WalParseError> {
        if rel.identity_columns.is_empty() {
            let pk_col_ids: Vec<ColumnId> = old_resolved.iter().map(|(c, _)| *c).collect();
            build_pk_from_resolved_strict(old_resolved, &pk_col_ids, "old tuple fallback key")
        } else {
            let pk_col_ids: Vec<ColumnId> = rel
                .identity_columns
                .iter()
                .map(|&i| rel.column_ids[i])
                .collect();
            build_pk_from_resolved_strict(old_resolved, &pk_col_ids, "replica identity")
        }
    }

    /// Handle Insert message.
    fn handle_insert(
        &self,
        cur: &mut Cursor<'_>,
        catalog: &dyn SchemaCatalog,
    ) -> Result<WalEvent, WalParseError> {
        let oid = cur.read_u32()?;
        let rel = self.get_relation(oid)?;

        // Consume the 'N' tag (new tuple marker)
        let tag = cur.read_u8()?;
        Self::expected_tag(tag, b'N', "INSERT tuple")?;

        let (new_row, new_resolved) = Self::parse_tuple_data(cur, &rel, true)?;
        let pk = pk_from_catalog_or_empty(&new_resolved, rel.table_id, catalog)?;

        Ok(insert_event(rel.table_id, pk, new_row))
    }

    /// Handle Update message.
    fn handle_update(
        &self,
        cur: &mut Cursor<'_>,
        catalog: &dyn SchemaCatalog,
    ) -> Result<WalEvent, WalParseError> {
        let oid = cur.read_u32()?;
        let rel = self.get_relation(oid)?;

        // Peek the next byte to decide if an old tuple is present.
        let tag = cur.read_u8()?;

        let (old_row, old_resolved) = if tag == b'K' || tag == b'O' {
            // Old tuple present (K = key, O = full old row)
            let (row, resolved) = if tag == b'K' {
                Self::parse_key_tuple_data(cur, &rel)?
            } else {
                Self::parse_tuple_data(cur, &rel, false)?
            };
            // Now consume the 'N' tag for the new tuple
            let n_tag = cur.read_u8()?;
            Self::expected_tag(n_tag, b'N', "UPDATE new tuple")?;
            (Some(row), resolved)
        } else if tag == b'N' {
            // tag is 'N' — no old tuple, directly the new tuple
            (None, Vec::new())
        } else {
            return Err(WalParseError::MalformedPayload(format!(
                "invalid UPDATE tuple tag: expected 0x4B ('K'), 0x4F ('O'), or 0x4E ('N'), got 0x{tag:02X}"
            )));
        };

        let (new_row, new_resolved) = Self::parse_tuple_data(cur, &rel, true)?;

        // PK: prefer identity columns from old row, then catalog fallback.
        let pk = if old_resolved.is_empty() {
            // No old row — extract PK from new row via catalog
            pk_from_catalog_or_empty(&new_resolved, rel.table_id, catalog)?
        } else {
            Self::pk_from_old_resolved(&rel, &old_resolved)?
        };

        let old_row_complete = super::old_row_is_complete(old_row.as_ref());

        Ok(update_event_with_old_row_completeness(
            rel.table_id,
            pk,
            old_row,
            new_row,
            old_row_complete,
        ))
    }

    /// Handle Delete message.
    fn handle_delete(
        &self,
        cur: &mut Cursor<'_>,
        _catalog: &dyn SchemaCatalog,
    ) -> Result<WalEvent, WalParseError> {
        let oid = cur.read_u32()?;
        let rel = self.get_relation(oid)?;

        // Read tag: 'K' (key) or 'O' (full old row)
        let tag = cur.read_u8()?;
        if tag != b'K' && tag != b'O' {
            return Err(WalParseError::MalformedPayload(format!(
                "invalid DELETE tuple tag: expected 0x4B ('K') or 0x4F ('O'), got 0x{tag:02X}"
            )));
        }

        let (old_row, old_resolved) = if tag == b'K' {
            Self::parse_key_tuple_data(cur, &rel)?
        } else {
            Self::parse_tuple_data(cur, &rel, false)?
        };

        // PK from identity columns
        let pk = Self::pk_from_old_resolved(&rel, &old_resolved)?;

        Ok(delete_event(rel.table_id, pk, old_row))
    }

    /// Handle Truncate message.
    fn handle_truncate(&self, cur: &mut Cursor<'_>) -> Result<Vec<WalEvent>, WalParseError> {
        let rel_count = Self::parse_relation_count(cur.read_i32()?, "Truncate")?;
        let _options = cur.read_u8()?;

        let mut events = Vec::with_capacity(rel_count);
        for _ in 0..rel_count {
            let oid = cur.read_u32()?;
            let rel = match self.get_relation(oid) {
                Ok(rel) => rel,
                Err(WalParseError::UnknownRelationOid(_)) => continue,
                Err(e) => return Err(e),
            };
            events.push(truncate_event(rel.table_id));
        }

        Ok(events)
    }
}

impl WalParser for PgOutputParser {
    fn parse_wal_message(
        &self,
        data: &[u8],
        catalog: &dyn SchemaCatalog,
    ) -> Result<Vec<WalEvent>, WalParseError> {
        if data.is_empty() {
            return Ok(vec![]);
        }

        let msg_type = data[0];
        let mut cur = Cursor::new(&data[1..]);

        match msg_type {
            // Metadata, 2PC, keepalive, and replication protocol messages — skip
            b'B' | b'C' | b'O' | b'Y' | b'M' | b'S' | b'E' | b'c' | b'A' | b'P' | b'K' | b'r'
            | b'b' | b'p' => Ok(vec![]),

            // Relation message — cache, no events
            b'R' => {
                self.handle_relation(&mut cur, catalog)?;
                Self::ensure_fully_consumed(&cur, "Relation message")?;
                Ok(vec![])
            }

            // Insert
            b'I' => {
                let event = self.handle_insert(&mut cur, catalog)?;
                Self::ensure_fully_consumed(&cur, "Insert message")?;
                Ok(vec![event])
            }

            // Update
            b'U' => {
                let event = self.handle_update(&mut cur, catalog)?;
                Self::ensure_fully_consumed(&cur, "Update message")?;
                Ok(vec![event])
            }

            // Delete
            b'D' => {
                let event = self.handle_delete(&mut cur, catalog)?;
                Self::ensure_fully_consumed(&cur, "Delete message")?;
                Ok(vec![event])
            }

            // Truncate
            b'T' => {
                let events = self.handle_truncate(&mut cur)?;
                Self::ensure_fully_consumed(&cur, "Truncate message")?;
                Ok(events)
            }

            other => {
                #[cfg(feature = "observability")]
                tracing::warn!("pgoutput: skipping unknown message type 0x{other:02X}");
                let _ = other;
                Ok(vec![])
            }
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::super::test_support::{
        orders_customer_catalog as orders_catalog,
        orders_customer_no_pk_catalog as orders_no_pk_catalog,
    };
    use super::*;

    // -- Binary message builders ---------------------------------------------

    fn push_cstring(buf: &mut Vec<u8>, s: &str) {
        buf.extend_from_slice(s.as_bytes());
        buf.push(0);
    }

    fn push_u8(buf: &mut Vec<u8>, v: u8) {
        buf.push(v);
    }

    fn push_i16(buf: &mut Vec<u8>, v: i16) {
        buf.extend_from_slice(&v.to_be_bytes());
    }

    fn push_i32(buf: &mut Vec<u8>, v: i32) {
        buf.extend_from_slice(&v.to_be_bytes());
    }

    fn push_u32(buf: &mut Vec<u8>, v: u32) {
        buf.extend_from_slice(&v.to_be_bytes());
    }

    /// Build a Relation ('R') message.
    fn build_relation_msg(
        oid: u32,
        namespace: &str,
        name: &str,
        columns: &[(&str, u32, u8)], // (name, type_oid, flags)
    ) -> Vec<u8> {
        let mut buf = vec![b'R'];
        push_u32(&mut buf, oid);
        push_cstring(&mut buf, namespace);
        push_cstring(&mut buf, name);
        push_u8(&mut buf, 0); // replica identity
        #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
        push_i16(&mut buf, columns.len() as i16);
        for &(col_name, type_oid, flags) in columns {
            push_u8(&mut buf, flags);
            push_cstring(&mut buf, col_name);
            push_u32(&mut buf, type_oid);
            push_i32(&mut buf, -1); // type modifier
        }
        buf
    }

    enum TupleCol {
        Null,
        Unchanged,
        Text(String),
    }

    /// Build raw tuple data bytes (without the leading tag like 'N'/'K'/'O').
    fn build_tuple_data(cols: &[TupleCol]) -> Vec<u8> {
        let mut buf = Vec::new();
        #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
        push_i16(&mut buf, cols.len() as i16);
        for col in cols {
            match col {
                TupleCol::Null => push_u8(&mut buf, b'n'),
                TupleCol::Unchanged => push_u8(&mut buf, b'u'),
                TupleCol::Text(s) => {
                    push_u8(&mut buf, b't');
                    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
                    push_i32(&mut buf, s.len() as i32);
                    buf.extend_from_slice(s.as_bytes());
                }
            }
        }
        buf
    }

    fn build_insert_msg(oid: u32, tuple: &[TupleCol]) -> Vec<u8> {
        let mut buf = vec![b'I'];
        push_u32(&mut buf, oid);
        push_u8(&mut buf, b'N');
        buf.extend_from_slice(&build_tuple_data(tuple));
        buf
    }

    fn build_update_msg_with_old(
        oid: u32,
        old_tag: u8,
        old_tuple: &[TupleCol],
        new_tuple: &[TupleCol],
    ) -> Vec<u8> {
        let mut buf = vec![b'U'];
        push_u32(&mut buf, oid);
        push_u8(&mut buf, old_tag);
        buf.extend_from_slice(&build_tuple_data(old_tuple));
        push_u8(&mut buf, b'N');
        buf.extend_from_slice(&build_tuple_data(new_tuple));
        buf
    }

    fn build_update_msg_no_old(oid: u32, new_tuple: &[TupleCol]) -> Vec<u8> {
        let mut buf = vec![b'U'];
        push_u32(&mut buf, oid);
        push_u8(&mut buf, b'N');
        buf.extend_from_slice(&build_tuple_data(new_tuple));
        buf
    }

    fn build_delete_msg(oid: u32, tag: u8, old_tuple: &[TupleCol]) -> Vec<u8> {
        let mut buf = vec![b'D'];
        push_u32(&mut buf, oid);
        push_u8(&mut buf, tag);
        buf.extend_from_slice(&build_tuple_data(old_tuple));
        buf
    }

    fn build_truncate_msg(option_bits: u8, relation_oids: &[u32]) -> Vec<u8> {
        let mut buf = vec![b'T'];
        #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
        push_i32(&mut buf, relation_oids.len() as i32);
        push_u8(&mut buf, option_bits);
        for &oid in relation_oids {
            push_u32(&mut buf, oid);
        }
        buf
    }

    /// Standard 4-column relation for orders table.
    fn orders_columns() -> Vec<(&'static str, u32, u8)> {
        vec![
            ("id", 23, 1),       // int4, identity
            ("customer", 25, 0), // text
            ("amount", 1700, 0), // numeric
            ("status", 25, 0),   // text
        ]
    }

    // -- Test 1: Relation caching + Insert (happy path) ----------------------

    #[test]
    fn relation_caching_and_insert() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();

        // Send Relation message
        let rel_msg = build_relation_msg(16384, "public", "orders", &orders_columns());
        let events = parser
            .parse_wal_message(&rel_msg, &catalog)
            .expect("relation should parse");
        assert!(events.is_empty());

        // Send Insert
        let insert_msg = build_insert_msg(
            16384,
            &[
                TupleCol::Text("1".into()),
                TupleCol::Text("alice".into()),
                TupleCol::Text("99.95".into()),
                TupleCol::Text("pending".into()),
            ],
        );
        let events = parser
            .parse_wal_message(&insert_msg, &catalog)
            .expect("insert should parse");

        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert_eq!(ev.kind, EventKind::Insert);
        assert_eq!(ev.table_id, 1);

        let new = ev.new_row.as_ref().expect("INSERT should have new_row");
        assert_eq!(new.get(0), Some(&Cell::Int(1)));
        assert_eq!(new.get(1), Some(&Cell::String(Arc::from("alice"))));
        assert_eq!(new.get(2), Some(&Cell::Float(99.95)));
        assert_eq!(new.get(3), Some(&Cell::String(Arc::from("pending"))));

        assert!(ev.old_row.is_none());

        // PK from catalog
        assert_eq!(ev.pk.columns.as_ref(), &[0]);
        assert_eq!(ev.pk.values.as_ref(), &[Cell::Int(1)]);

        assert!(ev.changed_columns.is_empty());
    }

    #[test]
    fn relation_with_out_of_range_catalog_column_id_errors() {
        let mut catalog = orders_catalog();
        // Force one relation column to resolve outside table arity.
        catalog.columns.insert((1, "status".to_string()), 99);
        let parser = PgOutputParser::new();

        let rel_msg = build_relation_msg(16384, "public", "orders", &orders_columns());
        let err = parser
            .parse_wal_message(&rel_msg, &catalog)
            .expect_err("out-of-range resolved column ID should fail");
        assert!(matches!(err, WalParseError::MalformedPayload(_)));
    }

    #[test]
    fn relation_with_duplicate_catalog_column_id_errors() {
        let mut catalog = orders_catalog();
        // Force two relation columns to resolve to the same catalog column id.
        catalog.columns.insert((1, "customer".to_string()), 0);
        let parser = PgOutputParser::new();

        let rel_msg = build_relation_msg(16384, "public", "orders", &orders_columns());
        let err = parser
            .parse_wal_message(&rel_msg, &catalog)
            .expect_err("duplicate resolved column ID should fail");
        assert!(matches!(err, WalParseError::MalformedPayload(_)));
    }

    #[test]
    fn relation_cache_is_bounded_and_evicts_oldest() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();

        let total_relations = MAX_CACHED_RELATIONS + 5;
        for i in 0..total_relations {
            let oid = 20_000_u32 + u32::try_from(i).expect("loop index fits u32");
            let rel_msg = build_relation_msg(oid, "public", "orders", &orders_columns());
            parser
                .parse_wal_message(&rel_msg, &catalog)
                .expect("relation should parse");
        }

        let cache = parser
            .relations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(cache.map.len(), MAX_CACHED_RELATIONS);
        drop(cache);

        let oldest_oid = 20_000_u32;
        let oldest_insert = build_insert_msg(
            oldest_oid,
            &[
                TupleCol::Text("1".into()),
                TupleCol::Text("alice".into()),
                TupleCol::Text("99.95".into()),
                TupleCol::Text("pending".into()),
            ],
        );
        let err = parser
            .parse_wal_message(&oldest_insert, &catalog)
            .expect_err("oldest relation should be evicted");
        assert!(matches!(err, WalParseError::UnknownRelationOid(oid) if oid == oldest_oid));

        let newest_oid = 20_000_u32 + u32::try_from(total_relations - 1).expect("index fits u32");
        let newest_insert = build_insert_msg(
            newest_oid,
            &[
                TupleCol::Text("1".into()),
                TupleCol::Text("alice".into()),
                TupleCol::Text("99.95".into()),
                TupleCol::Text("pending".into()),
            ],
        );
        let events = parser
            .parse_wal_message(&newest_insert, &catalog)
            .expect("newest relation should remain cached");
        assert_eq!(events.len(), 1);
    }

    // -- Test 2: Update with 'K' old key (DEFAULT replica identity) ----------

    #[test]
    fn update_with_key_old_tuple() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();

        let rel_msg = build_relation_msg(16384, "public", "orders", &orders_columns());
        parser
            .parse_wal_message(&rel_msg, &catalog)
            .expect("relation should parse");

        let update_msg = build_update_msg_with_old(
            16384,
            b'K',
            &[
                TupleCol::Text("1".into()),
                TupleCol::Unchanged,
                TupleCol::Unchanged,
                TupleCol::Unchanged,
            ],
            &[
                TupleCol::Text("1".into()),
                TupleCol::Text("alice".into()),
                TupleCol::Text("149.95".into()),
                TupleCol::Text("shipped".into()),
            ],
        );

        let events = parser
            .parse_wal_message(&update_msg, &catalog)
            .expect("update should parse");

        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert_eq!(ev.kind, EventKind::Update);

        assert!(ev.new_row.is_some());
        assert!(ev.old_row.is_some());

        // PK from identity column (id, flags=1)
        assert_eq!(ev.pk.columns.as_ref(), &[0]);
        assert_eq!(ev.pk.values.as_ref(), &[Cell::Int(1)]);
    }

    #[test]
    fn update_with_partial_key_tuple_keeps_changed_columns_empty_for_safety() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();

        let rel_msg = build_relation_msg(16384, "public", "orders", &orders_columns());
        parser
            .parse_wal_message(&rel_msg, &catalog)
            .expect("relation should parse");

        // Key tuple contains only the identity column. New tuple changes both id and amount.
        // Old row is partial, so changed-columns must be treated as unknown.
        let update_msg = build_update_msg_with_old(
            16384,
            b'K',
            &[TupleCol::Text("1".into())],
            &[
                TupleCol::Text("2".into()),
                TupleCol::Text("alice".into()),
                TupleCol::Text("149.95".into()),
                TupleCol::Text("shipped".into()),
            ],
        );

        let events = parser
            .parse_wal_message(&update_msg, &catalog)
            .expect("update should parse");
        assert_eq!(events.len(), 1);

        let ev = &events[0];
        assert_eq!(ev.kind, EventKind::Update);
        assert!(ev.old_row.is_some());
        assert!(
            ev.changed_columns.is_empty(),
            "partial key tuple must disable changed-columns pruning"
        );
    }

    #[test]
    fn update_with_key_old_tuple_sparse_identity_maps_correctly() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();

        // Identity is on `customer` (column index 1), not the first column.
        let cols: Vec<(&str, u32, u8)> = vec![
            ("id", 23, 0),
            ("customer", 25, 1),
            ("amount", 1700, 0),
            ("status", 25, 0),
        ];
        let rel_msg = build_relation_msg(16384, "public", "orders", &cols);
        parser
            .parse_wal_message(&rel_msg, &catalog)
            .expect("relation should parse");

        // Sparse key tuple where only the identity column value is present.
        let update_msg = build_update_msg_with_old(
            16384,
            b'K',
            &[TupleCol::Text("alice".into())],
            &[
                TupleCol::Text("1".into()),
                TupleCol::Text("alice".into()),
                TupleCol::Text("149.95".into()),
                TupleCol::Text("shipped".into()),
            ],
        );

        let events = parser
            .parse_wal_message(&update_msg, &catalog)
            .expect("sparse key tuple should map onto identity columns");
        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert_eq!(ev.kind, EventKind::Update);
        assert_eq!(ev.pk.columns.as_ref(), &[1]);
        assert_eq!(ev.pk.values.as_ref(), &[Cell::String(Arc::from("alice"))]);
    }

    // -- Test 3: Update with 'O' full old row (FULL replica identity) --------

    #[test]
    fn update_with_full_old_row() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();

        // All columns have flags=1 for FULL replica identity
        let full_cols: Vec<(&str, u32, u8)> = vec![
            ("id", 23, 1),
            ("customer", 25, 1),
            ("amount", 1700, 1),
            ("status", 25, 1),
        ];
        let rel_msg = build_relation_msg(16384, "public", "orders", &full_cols);
        parser
            .parse_wal_message(&rel_msg, &catalog)
            .expect("relation should parse");

        let update_msg = build_update_msg_with_old(
            16384,
            b'O',
            &[
                TupleCol::Text("1".into()),
                TupleCol::Text("alice".into()),
                TupleCol::Text("99.95".into()),
                TupleCol::Text("pending".into()),
            ],
            &[
                TupleCol::Text("1".into()),
                TupleCol::Text("alice".into()),
                TupleCol::Text("149.95".into()),
                TupleCol::Text("shipped".into()),
            ],
        );

        let events = parser
            .parse_wal_message(&update_msg, &catalog)
            .expect("update should parse");

        let ev = &events[0];
        assert_eq!(ev.kind, EventKind::Update);

        let old = ev.old_row.as_ref().expect("should have old_row");
        assert_eq!(old.get(0), Some(&Cell::Int(1)));
        assert_eq!(old.get(2), Some(&Cell::Float(99.95)));

        let new = ev.new_row.as_ref().expect("should have new_row");
        assert_eq!(new.get(2), Some(&Cell::Float(149.95)));
        assert_eq!(new.get(3), Some(&Cell::String(Arc::from("shipped"))));

        // PK from identity columns (all marked)
        assert_eq!(ev.pk.columns.as_ref(), &[0, 1, 2, 3]);
    }

    // -- Test 4: Update without old tuple ------------------------------------

    #[test]
    fn update_without_old_tuple() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();

        let rel_msg = build_relation_msg(16384, "public", "orders", &orders_columns());
        parser
            .parse_wal_message(&rel_msg, &catalog)
            .expect("relation should parse");

        let update_msg = build_update_msg_no_old(
            16384,
            &[
                TupleCol::Text("1".into()),
                TupleCol::Text("alice".into()),
                TupleCol::Text("149.95".into()),
                TupleCol::Text("shipped".into()),
            ],
        );

        let events = parser
            .parse_wal_message(&update_msg, &catalog)
            .expect("update should parse");

        let ev = &events[0];
        assert_eq!(ev.kind, EventKind::Update);
        assert!(ev.old_row.is_none());
        assert!(ev.changed_columns.is_empty());

        // PK from catalog (no old row)
        assert_eq!(ev.pk.columns.as_ref(), &[0]);
        assert_eq!(ev.pk.values.as_ref(), &[Cell::Int(1)]);
    }

    // -- Test 5: Delete with 'K' key ----------------------------------------

    #[test]
    fn delete_with_key() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();

        let rel_msg = build_relation_msg(16384, "public", "orders", &orders_columns());
        parser
            .parse_wal_message(&rel_msg, &catalog)
            .expect("relation should parse");

        let delete_msg = build_delete_msg(
            16384,
            b'K',
            &[
                TupleCol::Text("42".into()),
                TupleCol::Unchanged,
                TupleCol::Unchanged,
                TupleCol::Unchanged,
            ],
        );

        let events = parser
            .parse_wal_message(&delete_msg, &catalog)
            .expect("delete should parse");

        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert_eq!(ev.kind, EventKind::Delete);
        assert!(ev.new_row.is_none());
        assert!(ev.old_row.is_some());

        // PK from identity column
        assert_eq!(ev.pk.columns.as_ref(), &[0]);
        assert_eq!(ev.pk.values.as_ref(), &[Cell::Int(42)]);

        assert!(ev.changed_columns.is_empty());
    }

    #[test]
    fn delete_with_key_sparse_identity_maps_correctly() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();

        // Identity is on `customer` (column index 1), not the first column.
        let cols: Vec<(&str, u32, u8)> = vec![
            ("id", 23, 0),
            ("customer", 25, 1),
            ("amount", 1700, 0),
            ("status", 25, 0),
        ];
        let rel_msg = build_relation_msg(16384, "public", "orders", &cols);
        parser
            .parse_wal_message(&rel_msg, &catalog)
            .expect("relation should parse");

        let delete_msg = build_delete_msg(16384, b'K', &[TupleCol::Text("bob".into())]);

        let events = parser
            .parse_wal_message(&delete_msg, &catalog)
            .expect("sparse key tuple should map onto identity columns");
        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert_eq!(ev.kind, EventKind::Delete);
        assert_eq!(ev.pk.columns.as_ref(), &[1]);
        assert_eq!(ev.pk.values.as_ref(), &[Cell::String(Arc::from("bob"))]);
    }

    // -- Test 6: Delete with 'O' full old row --------------------------------

    #[test]
    fn delete_with_full_old_row() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();

        let rel_msg = build_relation_msg(16384, "public", "orders", &orders_columns());
        parser
            .parse_wal_message(&rel_msg, &catalog)
            .expect("relation should parse");

        let delete_msg = build_delete_msg(
            16384,
            b'O',
            &[
                TupleCol::Text("42".into()),
                TupleCol::Text("bob".into()),
                TupleCol::Text("50.00".into()),
                TupleCol::Text("cancelled".into()),
            ],
        );

        let events = parser
            .parse_wal_message(&delete_msg, &catalog)
            .expect("delete should parse");

        let ev = &events[0];
        assert_eq!(ev.kind, EventKind::Delete);

        let old = ev.old_row.as_ref().expect("should have old_row");
        assert_eq!(old.get(0), Some(&Cell::Int(42)));
        assert_eq!(old.get(1), Some(&Cell::String(Arc::from("bob"))));

        // PK from identity column (id)
        assert_eq!(ev.pk.columns.as_ref(), &[0]);
        assert_eq!(ev.pk.values.as_ref(), &[Cell::Int(42)]);
    }

    // -- Test 7: Metadata messages return empty vec --------------------------

    #[test]
    fn metadata_messages_return_empty() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();

        for &tag in b"BCOY" {
            let msg = vec![tag, 0, 0, 0, 0]; // tag + some padding
            let events = parser
                .parse_wal_message(&msg, &catalog)
                .expect("metadata should be skipped");
            assert!(events.is_empty(), "tag 0x{tag:02X} should return empty");
        }
    }

    #[test]
    fn non_row_control_messages_return_empty() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();

        // M = Logical decoding message; S/E/c/A = stream control/transaction wrappers.
        for &tag in b"MSEcA" {
            let msg = vec![tag, 0, 0, 0, 0];
            let events = parser
                .parse_wal_message(&msg, &catalog)
                .expect("non-row control message should be skipped");
            assert!(events.is_empty(), "tag 0x{tag:02X} should return empty");
        }
    }

    #[test]
    fn truncate_message_emits_events_per_relation() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();

        let relation_msg = build_relation_msg(16384, "public", "orders", &orders_columns());
        let events = parser
            .parse_wal_message(&relation_msg, &catalog)
            .expect("relation should parse");
        assert!(events.is_empty());

        let truncate_msg = build_truncate_msg(0, &[16384]);
        let events = parser
            .parse_wal_message(&truncate_msg, &catalog)
            .expect("truncate should parse");
        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert_eq!(ev.kind, EventKind::Truncate);
        assert_eq!(ev.table_id, 1);
        assert!(ev.pk.is_empty());
        assert!(ev.old_row.is_none());
        assert!(ev.new_row.is_none());
        assert!(ev.changed_columns.is_empty());
    }

    // -- Test 8: Empty input returns empty vec --------------------------------

    #[test]
    fn empty_input_returns_empty() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();

        let events = parser
            .parse_wal_message(&[], &catalog)
            .expect("empty should succeed");
        assert!(events.is_empty());
    }

    // -- Test 9: Unknown message type → skip ---------------------------------

    #[test]
    fn unknown_message_type_is_skipped() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();

        // 0xFF is not a known pgoutput message type; it must be skipped, not error.
        let msg = vec![0xFF, 0, 0, 0, 0];
        let events = parser
            .parse_wal_message(&msg, &catalog)
            .expect("unknown message type should be skipped, not error");
        assert!(
            events.is_empty(),
            "unknown message type should produce no output"
        );
    }

    // -- Test 10: Insert without preceding Relation → UnknownRelationOid -----

    #[test]
    fn insert_without_relation_error() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();

        let insert_msg = build_insert_msg(99999, &[TupleCol::Text("1".into())]);
        let err = parser
            .parse_wal_message(&insert_msg, &catalog)
            .expect_err("should fail");
        assert!(matches!(err, WalParseError::UnknownRelationOid(99999)));
    }

    // -- Test 11: Truncated messages → TruncatedMessage ----------------------

    #[test]
    fn truncated_message_error() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();

        // Insert with only 2 bytes after the type tag (needs at least 4 for OID)
        let msg = vec![b'I', 0, 0];
        let err = parser
            .parse_wal_message(&msg, &catalog)
            .expect_err("should fail");
        assert!(matches!(err, WalParseError::TruncatedMessage { .. }));
    }

    #[test]
    fn insert_with_trailing_bytes_errors() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();

        let rel_msg = build_relation_msg(16384, "public", "orders", &orders_columns());
        parser
            .parse_wal_message(&rel_msg, &catalog)
            .expect("relation should parse");

        let mut insert_msg = build_insert_msg(
            16384,
            &[
                TupleCol::Text("1".into()),
                TupleCol::Text("alice".into()),
                TupleCol::Text("99.95".into()),
                TupleCol::Text("pending".into()),
            ],
        );
        insert_msg.push(0xAA);
        insert_msg.push(0xBB);

        let err = parser
            .parse_wal_message(&insert_msg, &catalog)
            .expect_err("trailing bytes should fail");
        assert!(matches!(err, WalParseError::MalformedPayload(_)));
    }

    // -- Test 12: NULL columns ('n' tag) → Cell::Null ------------------------

    #[test]
    fn null_column_tag() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();

        let rel_msg = build_relation_msg(16384, "public", "orders", &orders_columns());
        parser
            .parse_wal_message(&rel_msg, &catalog)
            .expect("relation should parse");

        let insert_msg = build_insert_msg(
            16384,
            &[
                TupleCol::Text("1".into()),
                TupleCol::Null,
                TupleCol::Null,
                TupleCol::Text("active".into()),
            ],
        );

        let events = parser
            .parse_wal_message(&insert_msg, &catalog)
            .expect("insert should parse");

        let new = events[0].new_row.as_ref().expect("should have new_row");
        assert_eq!(new.get(0), Some(&Cell::Int(1)));
        assert_eq!(new.get(1), Some(&Cell::Null));
        assert_eq!(new.get(2), Some(&Cell::Null));
        assert_eq!(new.get(3), Some(&Cell::String(Arc::from("active"))));
    }

    // -- Test 13: Unchanged TOAST ('u' tag) → Cell::Missing in OLD tuple -----

    #[test]
    fn unchanged_toast_tag_in_old_key_tuple() {
        // 'u' (unchanged-TOAST) is only valid in old-image tuples (key tuples or full old rows).
        // It must produce Cell::Missing there, and be rejected in new-image tuples.
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();

        let rel_msg = build_relation_msg(16384, "public", "orders", &orders_columns());
        parser
            .parse_wal_message(&rel_msg, &catalog)
            .expect("relation should parse");

        // Update with 'K' old key (b'u' is valid here) and a clean new tuple (no b'u')
        // Columns: id(int4), customer(text), amount(numeric), status(text)
        let update_msg = build_update_msg_with_old(
            16384,
            b'K',
            &[
                TupleCol::Text("1".into()),
                TupleCol::Unchanged, // valid in old/key tuple (customer)
                TupleCol::Unchanged, // valid in old/key tuple (amount)
                TupleCol::Unchanged, // valid in old/key tuple (status)
            ],
            &[
                TupleCol::Text("1".into()),       // id
                TupleCol::Text("alice".into()),   // customer (text OID 25)
                TupleCol::Text("200.00".into()),  // amount (numeric OID 1700)
                TupleCol::Text("shipped".into()), // status (text OID 25)
            ],
        );

        let events = parser
            .parse_wal_message(&update_msg, &catalog)
            .expect("update should parse");

        let ev = &events[0];
        let old = ev.old_row.as_ref().expect("should have old_row");
        assert_eq!(old.get(1), Some(&Cell::Missing)); // unchanged TOAST in old is ok

        let new = ev.new_row.as_ref().expect("should have new_row");
        assert_eq!(new.get(1), Some(&Cell::String(Arc::from("alice"))));
        assert_eq!(new.get(2), Some(&Cell::Float(200.0)));
        assert_eq!(new.get(3), Some(&Cell::String(Arc::from("shipped"))));
    }

    // -- Test 14: Type conversion for various OIDs ---------------------------

    #[test]
    fn type_conversion_oids() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();

        // Custom columns with different types
        let cols: Vec<(&str, u32, u8)> = vec![
            ("id", 23, 1),       // int4
            ("customer", 25, 0), // text
            ("amount", 701, 0),  // float8
            ("status", 16, 0),   // bool
        ];
        let rel_msg = build_relation_msg(16384, "public", "orders", &cols);
        parser
            .parse_wal_message(&rel_msg, &catalog)
            .expect("relation should parse");

        let insert_msg = build_insert_msg(
            16384,
            &[
                TupleCol::Text("42".into()),
                TupleCol::Text("bob".into()),
                TupleCol::Text("3.15".into()),
                TupleCol::Text("t".into()),
            ],
        );

        let events = parser
            .parse_wal_message(&insert_msg, &catalog)
            .expect("insert should parse");

        let new = events[0].new_row.as_ref().expect("should have new_row");
        assert_eq!(new.get(0), Some(&Cell::Int(42)));
        assert_eq!(new.get(1), Some(&Cell::String(Arc::from("bob"))));
        assert_eq!(new.get(2), Some(&Cell::Float(3.15)));
        assert_eq!(new.get(3), Some(&Cell::Bool(true)));
    }

    #[test]
    fn type_conversion_invalid_typed_text_errors() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();

        let cols: Vec<(&str, u32, u8)> = vec![
            ("id", 23, 1),       // int4
            ("customer", 25, 0), // text
            ("amount", 701, 0),  // float8
            ("status", 16, 0),   // bool
        ];
        let rel_msg = build_relation_msg(16384, "public", "orders", &cols);
        parser
            .parse_wal_message(&rel_msg, &catalog)
            .expect("relation should parse");

        let insert_msg = build_insert_msg(
            16384,
            &[
                TupleCol::Text("not-an-int".into()),
                TupleCol::Text("bob".into()),
                TupleCol::Text("3.15".into()),
                TupleCol::Text("t".into()),
            ],
        );

        let err = parser
            .parse_wal_message(&insert_msg, &catalog)
            .expect_err("invalid int4 tuple text should fail");
        assert!(matches!(err, WalParseError::MalformedPayload(_)));
    }

    // -- Test 15: changed_columns computed correctly for FULL update ----------

    #[test]
    fn changed_columns_full_update() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();

        // All columns have flags=1 for FULL replica identity
        let full_cols: Vec<(&str, u32, u8)> = vec![
            ("id", 23, 1),
            ("customer", 25, 1),
            ("amount", 1700, 1),
            ("status", 25, 1),
        ];
        let rel_msg = build_relation_msg(16384, "public", "orders", &full_cols);
        parser
            .parse_wal_message(&rel_msg, &catalog)
            .expect("relation should parse");

        let update_msg = build_update_msg_with_old(
            16384,
            b'O',
            &[
                TupleCol::Text("1".into()),
                TupleCol::Text("alice".into()),
                TupleCol::Text("99.95".into()),
                TupleCol::Text("pending".into()),
            ],
            &[
                TupleCol::Text("1".into()),
                TupleCol::Text("alice".into()),
                TupleCol::Text("149.95".into()),
                TupleCol::Text("shipped".into()),
            ],
        );

        let events = parser
            .parse_wal_message(&update_msg, &catalog)
            .expect("update should parse");

        let ev = &events[0];
        let changed: Vec<ColumnId> = ev.changed_columns.to_vec();
        assert!(changed.contains(&2), "amount should be changed");
        assert!(changed.contains(&3), "status should be changed");
        assert!(!changed.contains(&0), "id should NOT be changed");
        assert!(!changed.contains(&1), "customer should NOT be changed");
    }

    // -- Test 16: Trait object safety ----------------------------------------

    #[test]
    fn trait_object_compiles() {
        let parser: &dyn WalParser = &PgOutputParser::new();
        let catalog = orders_catalog();

        // Should compile and work as a trait object
        let rel_msg = build_relation_msg(16384, "public", "orders", &orders_columns());
        let result = parser.parse_wal_message(&rel_msg, &catalog);
        assert!(result.is_ok());
    }

    // -- Test 17: Thread safety (compile-time Send + Sync check) -------------

    #[test]
    fn send_sync_check() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<PgOutputParser>();
    }

    // -- Test: Insert without catalog PK → empty PK --------------------------

    #[test]
    fn insert_no_catalog_pk() {
        let catalog = orders_no_pk_catalog();
        let parser = PgOutputParser::new();

        let rel_msg = build_relation_msg(16384, "public", "orders", &orders_columns());
        parser
            .parse_wal_message(&rel_msg, &catalog)
            .expect("relation should parse");

        let insert_msg = build_insert_msg(
            16384,
            &[
                TupleCol::Text("1".into()),
                TupleCol::Text("alice".into()),
                TupleCol::Text("99.95".into()),
                TupleCol::Text("pending".into()),
            ],
        );

        let events = parser
            .parse_wal_message(&insert_msg, &catalog)
            .expect("insert should parse");

        let ev = &events[0];
        assert!(ev.pk.columns.is_empty());
        assert!(ev.pk.values.is_empty());
    }

    // -- Test: Truncated relation message ------------------------------------

    #[test]
    fn truncated_relation_message() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();

        // Relation message with only the OID (truncated before namespace)
        let msg = vec![b'R', 0, 0, 0x40, 0x00];
        let err = parser
            .parse_wal_message(&msg, &catalog)
            .expect_err("should fail");
        assert!(matches!(err, WalParseError::TruncatedMessage { .. }));
    }

    // -- Test: Delete without identity columns → use all columns as PK -------

    #[test]
    fn delete_no_identity_columns() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();

        // No identity flags (all flags = 0)
        let no_identity_cols: Vec<(&str, u32, u8)> = vec![
            ("id", 23, 0),
            ("customer", 25, 0),
            ("amount", 1700, 0),
            ("status", 25, 0),
        ];
        let rel_msg = build_relation_msg(16384, "public", "orders", &no_identity_cols);
        parser
            .parse_wal_message(&rel_msg, &catalog)
            .expect("relation should parse");

        let delete_msg = build_delete_msg(
            16384,
            b'O',
            &[
                TupleCol::Text("1".into()),
                TupleCol::Text("alice".into()),
                TupleCol::Text("99.95".into()),
                TupleCol::Text("pending".into()),
            ],
        );

        let events = parser
            .parse_wal_message(&delete_msg, &catalog)
            .expect("delete should parse");

        let ev = &events[0];
        // All columns used as PK since no identity columns
        assert_eq!(ev.pk.columns.len(), 4);
    }

    #[test]
    fn delete_no_identity_columns_with_missing_old_value_errors() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();

        let no_identity_cols: Vec<(&str, u32, u8)> = vec![
            ("id", 23, 0),
            ("customer", 25, 0),
            ("amount", 1700, 0),
            ("status", 25, 0),
        ];
        let rel_msg = build_relation_msg(16384, "public", "orders", &no_identity_cols);
        parser
            .parse_wal_message(&rel_msg, &catalog)
            .expect("relation should parse");

        let delete_msg = build_delete_msg(
            16384,
            b'O',
            &[
                TupleCol::Text("1".into()),
                TupleCol::Text("alice".into()),
                TupleCol::Unchanged,
                TupleCol::Text("pending".into()),
            ],
        );

        let err = parser
            .parse_wal_message(&delete_msg, &catalog)
            .expect_err("missing old tuple value should fail for fallback PK");
        assert!(matches!(err, WalParseError::MalformedPayload(_)));
    }

    #[test]
    fn update_no_identity_columns_uses_full_old_tuple_as_pk() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();

        let no_identity_cols: Vec<(&str, u32, u8)> = vec![
            ("id", 23, 0),
            ("customer", 25, 0),
            ("amount", 1700, 0),
            ("status", 25, 0),
        ];
        let rel_msg = build_relation_msg(16384, "public", "orders", &no_identity_cols);
        parser
            .parse_wal_message(&rel_msg, &catalog)
            .expect("relation should parse");

        let update_msg = build_update_msg_with_old(
            16384,
            b'O',
            &[
                TupleCol::Text("1".into()),
                TupleCol::Text("alice".into()),
                TupleCol::Text("99.95".into()),
                TupleCol::Text("pending".into()),
            ],
            &[
                TupleCol::Text("1".into()),
                TupleCol::Text("alice".into()),
                TupleCol::Text("120.00".into()),
                TupleCol::Text("paid".into()),
            ],
        );

        let events = parser
            .parse_wal_message(&update_msg, &catalog)
            .expect("update should parse");

        let ev = &events[0];
        assert_eq!(ev.kind, EventKind::Update);
        assert_eq!(ev.pk.columns.len(), 4);
    }

    #[test]
    fn relation_negative_column_count_does_not_panic() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();

        let mut msg = Vec::new();
        msg.push(b'R');
        push_u32(&mut msg, 16384);
        push_cstring(&mut msg, "public");
        push_cstring(&mut msg, "orders");
        msg.push(b'd');
        push_i16(&mut msg, -1);

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            parser.parse_wal_message(&msg, &catalog)
        }));

        assert!(
            result.is_ok(),
            "parser must not panic on malformed relation"
        );
        let parse_result = result.expect("catch_unwind should be Ok");
        assert!(
            matches!(parse_result, Err(WalParseError::MalformedPayload(_))),
            "parser should return MalformedPayload"
        );
    }

    #[test]
    fn tuple_negative_column_count_does_not_panic() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();

        let rel_msg = build_relation_msg(16384, "public", "orders", &orders_columns());
        parser
            .parse_wal_message(&rel_msg, &catalog)
            .expect("relation should parse");

        let mut msg = Vec::new();
        msg.push(b'I');
        push_u32(&mut msg, 16384);
        msg.push(b'N');
        push_i16(&mut msg, -1);

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            parser.parse_wal_message(&msg, &catalog)
        }));

        assert!(result.is_ok(), "parser must not panic on malformed tuple");
        let parse_result = result.expect("catch_unwind should be Ok");
        assert!(
            matches!(parse_result, Err(WalParseError::MalformedPayload(_))),
            "parser should return MalformedPayload"
        );
    }

    #[test]
    fn insert_invalid_tuple_tag_does_not_panic() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();

        let rel_msg = build_relation_msg(16384, "public", "orders", &orders_columns());
        parser
            .parse_wal_message(&rel_msg, &catalog)
            .expect("relation should parse");

        let mut msg = build_insert_msg(16384, &[TupleCol::Text("1".into())]);
        msg[5] = b'X';

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            parser.parse_wal_message(&msg, &catalog)
        }));

        assert!(
            result.is_ok(),
            "parser must not panic on invalid insert tuple tag"
        );
        let parse_result = result.expect("catch_unwind should be Ok");
        assert!(
            matches!(parse_result, Err(WalParseError::MalformedPayload(_))),
            "parser should return MalformedPayload"
        );
    }

    #[test]
    fn update_invalid_initial_tuple_tag_does_not_panic() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();

        let rel_msg = build_relation_msg(16384, "public", "orders", &orders_columns());
        parser
            .parse_wal_message(&rel_msg, &catalog)
            .expect("relation should parse");

        let mut msg = build_update_msg_no_old(16384, &[TupleCol::Text("1".into())]);
        msg[5] = b'X';

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            parser.parse_wal_message(&msg, &catalog)
        }));

        assert!(
            result.is_ok(),
            "parser must not panic on invalid update tuple tag"
        );
        let parse_result = result.expect("catch_unwind should be Ok");
        assert!(
            matches!(parse_result, Err(WalParseError::MalformedPayload(_))),
            "parser should return MalformedPayload"
        );
    }

    #[test]
    fn update_invalid_new_tuple_tag_after_old_does_not_panic() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();

        let rel_msg = build_relation_msg(16384, "public", "orders", &orders_columns());
        parser
            .parse_wal_message(&rel_msg, &catalog)
            .expect("relation should parse");

        let old_cols = [
            TupleCol::Text("1".into()),
            TupleCol::Unchanged,
            TupleCol::Unchanged,
            TupleCol::Unchanged,
        ];
        let mut msg =
            build_update_msg_with_old(16384, b'K', &old_cols, &[TupleCol::Text("1".into())]);
        let new_tag_idx = 1 + 4 + 1 + build_tuple_data(&old_cols).len();
        msg[new_tag_idx] = b'X';

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            parser.parse_wal_message(&msg, &catalog)
        }));

        assert!(
            result.is_ok(),
            "parser must not panic on invalid post-old update tuple tag"
        );
        let parse_result = result.expect("catch_unwind should be Ok");
        assert!(
            matches!(parse_result, Err(WalParseError::MalformedPayload(_))),
            "parser should return MalformedPayload"
        );
    }

    #[test]
    fn delete_invalid_tuple_tag_returns_malformed_payload() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();

        let rel_msg = build_relation_msg(16384, "public", "orders", &orders_columns());
        parser
            .parse_wal_message(&rel_msg, &catalog)
            .expect("relation should parse");

        let msg = build_delete_msg(16384, b'X', &[TupleCol::Text("1".into())]);
        let err = parser
            .parse_wal_message(&msg, &catalog)
            .expect_err("should fail");
        assert!(matches!(err, WalParseError::MalformedPayload(_)));
    }
    // -- B3: skip 2PC/keepalive/replication protocol messages ----------------

    #[test]
    fn skip_2pc_keepalive_messages() {
        let parser = PgOutputParser::new();
        let catalog = orders_catalog();
        // b'P' = Prepare (2PC)
        assert!(
            parser
                .parse_wal_message(b"P", &catalog)
                .expect("P should parse")
                .is_empty(),
            "P should yield no events"
        );
        // b'K' = Keepalive
        assert!(
            parser
                .parse_wal_message(b"K", &catalog)
                .expect("K should parse")
                .is_empty(),
            "K should yield no events"
        );
        // b'r' = standby status update
        assert!(
            parser
                .parse_wal_message(b"r", &catalog)
                .expect("r should parse")
                .is_empty(),
            "r should yield no events"
        );
        // b'b' = begin prepared
        assert!(
            parser
                .parse_wal_message(b"b", &catalog)
                .expect("b should parse")
                .is_empty(),
            "b should yield no events"
        );
        // b'p' = commit prepared
        assert!(
            parser
                .parse_wal_message(b"p", &catalog)
                .expect("p should parse")
                .is_empty(),
            "p should yield no events"
        );
    }

    // -- B4: under-arity normal tuples are rejected --------------------------

    #[test]
    fn insert_underarity_tuple_rejected() {
        // Register a 4-column relation (orders), then send an INSERT with only 1 column.
        let parser = PgOutputParser::new();
        let catalog = orders_catalog();

        let rel_msg = build_relation_msg(16384, "public", "orders", &orders_columns());
        parser
            .parse_wal_message(&rel_msg, &catalog)
            .expect("relation should parse");

        // Build an INSERT with only 1 column in the tuple (arity mismatch: 4 expected)
        let insert_msg = build_insert_msg(16384, &[TupleCol::Text("1".into())]);
        let err = parser
            .parse_wal_message(&insert_msg, &catalog)
            .expect_err("under-arity tuple should fail");
        assert!(
            matches!(err, WalParseError::ArityMismatch { .. })
                || matches!(err, WalParseError::MalformedPayload(_)),
            "Expected ArityMismatch or MalformedPayload, got: {err:?}"
        );
    }

    // -- B5: LRU eviction boundary -------------------------------------------

    #[test]
    fn lru_eviction_boundary() {
        // MAX_CACHED_RELATIONS = 32 in test mode.
        // Register 33 relations (OIDs 0..=32), then OID 0 should be evicted.
        let parser = PgOutputParser::new();
        let catalog = orders_catalog();

        // Register 33 relations: OIDs 0..=32
        for oid in 0u32..=32u32 {
            let rel_msg = build_relation_msg(oid, "public", "orders", &orders_columns());
            parser
                .parse_wal_message(&rel_msg, &catalog)
                .expect("relation should parse");
        }

        // Now try to INSERT using OID 0 — it should have been evicted
        let insert_msg = build_insert_msg(
            0,
            &[
                TupleCol::Text("1".into()),
                TupleCol::Text("alice".into()),
                TupleCol::Text("99.95".into()),
                TupleCol::Text("pending".into()),
            ],
        );
        let err = parser
            .parse_wal_message(&insert_msg, &catalog)
            .expect_err("OID 0 should have been evicted");
        assert!(
            matches!(err, WalParseError::UnknownRelationOid(0)),
            "Expected UnknownRelationOid(0), got: {err:?}"
        );
    }

    // -- B6: TRUNCATE edge cases: zero relations, unknown OID ----------------

    #[test]
    fn truncate_zero_relations() {
        // TRUNCATE with rel_count=0 should produce no events
        let parser = PgOutputParser::new();
        let catalog = orders_catalog();

        let msg = build_truncate_msg(0, &[]);
        let events = parser
            .parse_wal_message(&msg, &catalog)
            .expect("zero-relation truncate should parse");
        assert!(
            events.is_empty(),
            "Expected empty events for zero-relation truncate"
        );
    }

    #[test]
    fn truncate_unknown_oid_is_skipped() {
        // TRUNCATE referencing an unknown OID should silently skip that relation
        let parser = PgOutputParser::new();
        let catalog = orders_catalog();

        let msg = build_truncate_msg(0, &[9999]);
        let events = parser
            .parse_wal_message(&msg, &catalog)
            .expect("unknown OID in TRUNCATE should be skipped, not error");
        assert!(
            events.is_empty(),
            "Expected empty events when all OIDs are unknown"
        );
    }

    #[test]
    fn truncate_skips_unknown_oid_emits_known_ones() {
        // A 3-OID TRUNCATE where OID[1] is unknown: events for OID[0] and OID[2] should be emitted.
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();

        // Register two known relations
        let rel_a = build_relation_msg(100, "public", "orders", &orders_columns());
        parser
            .parse_wal_message(&rel_a, &catalog)
            .expect("relation A should parse");

        // Build a second catalog entry for a second known table
        let mut catalog2 = orders_catalog();
        catalog2.tables.insert("public.items".to_string(), (2, 1));
        catalog2.tables.insert("items".to_string(), (2, 1));
        catalog2.columns.insert((2, "sku".to_string()), 0);

        // Register second relation using the extended catalog
        let rel_b = build_relation_msg(200, "public", "items", &[("sku", 25, 1)]);
        parser
            .parse_wal_message(&rel_b, &catalog2)
            .expect("relation B should parse");

        // OID 100 (known), OID 9999 (unknown), OID 200 (known)
        let truncate_msg = build_truncate_msg(0, &[100, 9999, 200]);
        let events = parser
            .parse_wal_message(&truncate_msg, &catalog2)
            .expect("truncate with one unknown OID should succeed");

        assert_eq!(events.len(), 2, "Should emit events for both known OIDs");
        assert!(events.iter().any(|e| e.table_id == 1));
        assert!(events.iter().any(|e| e.table_id == 2));
        assert!(events.iter().all(|e| e.kind == EventKind::Truncate));
    }

    // -- B7: UnknownTupleTag error path --------------------------------------

    // -- A2: unchanged-TOAST tag 'u' must be rejected in new-image tuples ----

    #[test]
    fn insert_with_unchanged_toast_tag_is_rejected() {
        // Build an INSERT message where a column's tuple-data tag is b'u' (unchanged-TOAST).
        // This tag is only valid in old-image tuples; it must be rejected in new-image tuples.
        let parser = PgOutputParser::new();
        let catalog = orders_catalog();

        let rel_msg = build_relation_msg(16384, "public", "orders", &orders_columns());
        parser
            .parse_wal_message(&rel_msg, &catalog)
            .expect("relation should parse");

        let insert_msg = build_insert_msg(
            16384,
            &[
                TupleCol::Text("1".into()),
                TupleCol::Unchanged, // 'u' tag — invalid in a new-image tuple
                TupleCol::Text("50.0".into()),
                TupleCol::Text("pending".into()),
            ],
        );

        let err = parser
            .parse_wal_message(&insert_msg, &catalog)
            .expect_err("unchanged-TOAST tag in INSERT new-tuple should fail");
        assert!(
            matches!(err, WalParseError::MalformedPayload(_)),
            "Expected MalformedPayload, got: {err:?}"
        );
    }

    #[test]
    fn unknown_tuple_tag_errors() {
        // Build an INSERT whose tuple-data column tag is 0xFF (not 'n', 'u', or 't')
        let parser = PgOutputParser::new();
        let catalog = orders_catalog();

        let rel_msg = build_relation_msg(16384, "public", "orders", &orders_columns());
        parser
            .parse_wal_message(&rel_msg, &catalog)
            .expect("relation should parse");

        // Build a raw INSERT message:
        //   b'I', OID(4 bytes), b'N', num_cols(i16=4), then tag 0xFF for col 0
        let mut msg = vec![b'I'];
        push_u32(&mut msg, 16384u32);
        push_u8(&mut msg, b'N'); // new tuple tag
        push_i16(&mut msg, 4i16); // 4 columns matching arity
        push_u8(&mut msg, 0xFF); // unknown column data tag

        let err = parser
            .parse_wal_message(&msg, &catalog)
            .expect_err("unknown tuple tag should fail");
        assert!(
            matches!(err, WalParseError::UnknownTupleTag(0xFF)),
            "Expected UnknownTupleTag(0xFF), got: {err:?}"
        );
    }
}
