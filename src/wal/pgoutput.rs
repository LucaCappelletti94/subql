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

use super::pg_type::text_to_cell;
use super::{build_pk_from_resolved, changed_columns, resolve_table, WalParseError, WalParser};
use crate::{Cell, ColumnId, EventKind, PrimaryKey, RowImage, SchemaCatalog, TableId, WalEvent};

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

    const fn check(&self, n: usize) -> Result<(), WalParseError> {
        if self.remaining() < n {
            return Err(WalParseError::TruncatedMessage {
                expected: self.pos + n,
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
    relations: Mutex<AHashMap<u32, CachedRelation>>,
}

impl Default for PgOutputParser {
    fn default() -> Self {
        Self {
            relations: Mutex::new(AHashMap::new()),
        }
    }
}

impl PgOutputParser {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
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
        let num_columns = cur.read_i16()?;

        #[allow(clippy::cast_sign_loss)]
        let mut columns = Vec::with_capacity(num_columns as usize);
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
        for col in &columns {
            let col_id = catalog.column_id(table_id, &col.name).ok_or_else(|| {
                WalParseError::UnknownColumn {
                    table_id,
                    column: col.name.clone(),
                }
            })?;
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
        map.get(&oid)
            .cloned()
            .ok_or(WalParseError::UnknownRelationOid(oid))
    }

    /// Parse a TupleData section, returning a `RowImage` and resolved pairs.
    fn parse_tuple_data(
        cur: &mut Cursor<'_>,
        rel: &CachedRelation,
    ) -> Result<(RowImage, Vec<(ColumnId, Cell)>), WalParseError> {
        let num_columns = cur.read_i16()?;
        let mut cells = vec![Cell::Missing; rel.arity];
        #[allow(clippy::cast_sign_loss)]
        let mut resolved = Vec::with_capacity(num_columns as usize);

        #[allow(clippy::cast_sign_loss)]
        for i in 0..num_columns as usize {
            let tag = cur.read_u8()?;
            let cell = match tag {
                b'n' => Cell::Null,
                b'u' => Cell::Missing,
                b't' => {
                    #[allow(clippy::cast_sign_loss)]
                    let len = cur.read_i32()? as usize;
                    let bytes = cur.read_bytes(len)?;
                    let text = std::str::from_utf8(bytes)
                        .map_err(|e| WalParseError::InvalidUtf8(e.to_string()))?;
                    text_to_cell(text, rel.columns[i].type_oid)
                }
                other => return Err(WalParseError::UnknownTupleTag(other)),
            };

            let col_id = rel.column_ids[i];
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
        debug_assert_eq!(tag, b'N');

        let (new_row, new_resolved) = Self::parse_tuple_data(cur, &rel)?;

        let pk = catalog.primary_key_columns(rel.table_id).map_or_else(
            || PrimaryKey {
                columns: Arc::from([]),
                values: Arc::from([]),
            },
            |pk_cols| build_pk_from_resolved(&new_resolved, pk_cols),
        );

        Ok(WalEvent {
            kind: EventKind::Insert,
            table_id: rel.table_id,
            pk,
            old_row: None,
            new_row: Some(new_row),
            changed_columns: Arc::from([]),
        })
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
            let (row, resolved) = Self::parse_tuple_data(cur, &rel)?;
            // Now consume the 'N' tag for the new tuple
            let n_tag = cur.read_u8()?;
            debug_assert_eq!(n_tag, b'N');
            (Some(row), resolved)
        } else {
            // tag is 'N' — no old tuple, directly the new tuple
            debug_assert_eq!(tag, b'N');
            (None, Vec::new())
        };

        let (new_row, new_resolved) = Self::parse_tuple_data(cur, &rel)?;

        // PK: prefer identity columns from old row, then catalog fallback.
        let pk = if old_resolved.is_empty() {
            // No old row — extract PK from new row via catalog
            catalog.primary_key_columns(rel.table_id).map_or_else(
                || PrimaryKey {
                    columns: Arc::from([]),
                    values: Arc::from([]),
                },
                |pk_cols| build_pk_from_resolved(&new_resolved, pk_cols),
            )
        } else if rel.identity_columns.is_empty() {
            // No identity columns marked — use all old columns as PK
            let cols: Vec<ColumnId> = old_resolved.iter().map(|(c, _)| *c).collect();
            let vals: Vec<Cell> = old_resolved.iter().map(|(_, v)| v.clone()).collect();
            PrimaryKey {
                columns: Arc::from(cols),
                values: Arc::from(vals),
            }
        } else {
            // Use identity columns from old row
            let pk_col_ids: Vec<ColumnId> = rel
                .identity_columns
                .iter()
                .map(|&i| rel.column_ids[i])
                .collect();
            build_pk_from_resolved(&old_resolved, &pk_col_ids)
        };

        let changed = if let (Some(ref old), new) = (&old_row, &new_row) {
            changed_columns(old, new)
        } else {
            Vec::new()
        };

        Ok(WalEvent {
            kind: EventKind::Update,
            table_id: rel.table_id,
            pk,
            old_row,
            new_row: Some(new_row),
            changed_columns: Arc::from(changed),
        })
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
        let _tag = cur.read_u8()?;

        let (old_row, old_resolved) = Self::parse_tuple_data(cur, &rel)?;

        // PK from identity columns
        let pk = if rel.identity_columns.is_empty() {
            // No identity columns — use all columns
            let cols: Vec<ColumnId> = old_resolved.iter().map(|(c, _)| *c).collect();
            let vals: Vec<Cell> = old_resolved.iter().map(|(_, v)| v.clone()).collect();
            PrimaryKey {
                columns: Arc::from(cols),
                values: Arc::from(vals),
            }
        } else {
            let pk_col_ids: Vec<ColumnId> = rel
                .identity_columns
                .iter()
                .map(|&i| rel.column_ids[i])
                .collect();
            build_pk_from_resolved(&old_resolved, &pk_col_ids)
        };

        Ok(WalEvent {
            kind: EventKind::Delete,
            table_id: rel.table_id,
            pk,
            old_row: Some(old_row),
            new_row: None,
            changed_columns: Arc::from([]),
        })
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
            // Metadata messages — skip
            b'B' | b'C' | b'O' | b'Y' | b'T' => Ok(vec![]),

            // Relation message — cache, no events
            b'R' => {
                self.handle_relation(&mut cur, catalog)?;
                Ok(vec![])
            }

            // Insert
            b'I' => {
                let event = self.handle_insert(&mut cur, catalog)?;
                Ok(vec![event])
            }

            // Update
            b'U' => {
                let event = self.handle_update(&mut cur, catalog)?;
                Ok(vec![event])
            }

            // Delete
            b'D' => {
                let event = self.handle_delete(&mut cur, catalog)?;
                Ok(vec![event])
            }

            other => Err(WalParseError::UnknownEventKind(format!(
                "pgoutput message type: 0x{other:02X}"
            ))),
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    // -- Test catalog --------------------------------------------------------

    struct TestCatalog {
        tables: HashMap<String, (TableId, usize)>,
        columns: HashMap<(TableId, String), ColumnId>,
        primary_keys: HashMap<TableId, Vec<ColumnId>>,
    }

    impl TestCatalog {
        fn orders() -> Self {
            let mut tables = HashMap::new();
            tables.insert("orders".to_string(), (1, 4));
            tables.insert("public.orders".to_string(), (1, 4));

            let mut columns = HashMap::new();
            columns.insert((1, "id".to_string()), 0);
            columns.insert((1, "customer".to_string()), 1);
            columns.insert((1, "amount".to_string()), 2);
            columns.insert((1, "status".to_string()), 3);

            let mut primary_keys = HashMap::new();
            primary_keys.insert(1, vec![0]); // id is PK

            Self {
                tables,
                columns,
                primary_keys,
            }
        }

        fn orders_no_pk() -> Self {
            let mut cat = Self::orders();
            cat.primary_keys.clear();
            cat
        }
    }

    impl SchemaCatalog for TestCatalog {
        fn table_id(&self, table_name: &str) -> Option<TableId> {
            self.tables.get(table_name).map(|(id, _)| *id)
        }

        fn column_id(&self, table_id: TableId, column_name: &str) -> Option<ColumnId> {
            self.columns
                .get(&(table_id, column_name.to_string()))
                .copied()
        }

        fn table_arity(&self, table_id: TableId) -> Option<usize> {
            self.tables
                .values()
                .find(|(id, _)| *id == table_id)
                .map(|(_, arity)| *arity)
        }

        fn schema_fingerprint(&self, _table_id: TableId) -> Option<u64> {
            Some(0)
        }

        fn primary_key_columns(&self, table_id: TableId) -> Option<&[ColumnId]> {
            self.primary_keys.get(&table_id).map(Vec::as_slice)
        }
    }

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
        #[allow(clippy::cast_possible_truncation)]
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
        #[allow(clippy::cast_possible_truncation)]
        push_i16(&mut buf, cols.len() as i16);
        for col in cols {
            match col {
                TupleCol::Null => push_u8(&mut buf, b'n'),
                TupleCol::Unchanged => push_u8(&mut buf, b'u'),
                TupleCol::Text(s) => {
                    push_u8(&mut buf, b't');
                    #[allow(clippy::cast_possible_truncation)]
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
        let catalog = TestCatalog::orders();
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

    // -- Test 2: Update with 'K' old key (DEFAULT replica identity) ----------

    #[test]
    fn update_with_key_old_tuple() {
        let catalog = TestCatalog::orders();
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

    // -- Test 3: Update with 'O' full old row (FULL replica identity) --------

    #[test]
    fn update_with_full_old_row() {
        let catalog = TestCatalog::orders();
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
        let catalog = TestCatalog::orders();
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
        let catalog = TestCatalog::orders();
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

    // -- Test 6: Delete with 'O' full old row --------------------------------

    #[test]
    fn delete_with_full_old_row() {
        let catalog = TestCatalog::orders();
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
        let catalog = TestCatalog::orders();
        let parser = PgOutputParser::new();

        for &tag in &[b'B', b'C', b'O', b'Y', b'T'] {
            let msg = vec![tag, 0, 0, 0, 0]; // tag + some padding
            let events = parser
                .parse_wal_message(&msg, &catalog)
                .expect("metadata should be skipped");
            assert!(events.is_empty(), "tag 0x{tag:02X} should return empty");
        }
    }

    // -- Test 8: Empty input returns empty vec --------------------------------

    #[test]
    fn empty_input_returns_empty() {
        let catalog = TestCatalog::orders();
        let parser = PgOutputParser::new();

        let events = parser
            .parse_wal_message(&[], &catalog)
            .expect("empty should succeed");
        assert!(events.is_empty());
    }

    // -- Test 9: Unknown message type → error --------------------------------

    #[test]
    fn unknown_message_type_error() {
        let catalog = TestCatalog::orders();
        let parser = PgOutputParser::new();

        let msg = vec![0xFF, 0, 0, 0, 0];
        let err = parser
            .parse_wal_message(&msg, &catalog)
            .expect_err("should fail");
        assert!(matches!(err, WalParseError::UnknownEventKind(_)));
    }

    // -- Test 10: Insert without preceding Relation → UnknownRelationOid -----

    #[test]
    fn insert_without_relation_error() {
        let catalog = TestCatalog::orders();
        let parser = PgOutputParser::new();

        let insert_msg = build_insert_msg(
            99999,
            &[TupleCol::Text("1".into())],
        );
        let err = parser
            .parse_wal_message(&insert_msg, &catalog)
            .expect_err("should fail");
        assert!(matches!(err, WalParseError::UnknownRelationOid(99999)));
    }

    // -- Test 11: Truncated messages → TruncatedMessage ----------------------

    #[test]
    fn truncated_message_error() {
        let catalog = TestCatalog::orders();
        let parser = PgOutputParser::new();

        // Insert with only 2 bytes after the type tag (needs at least 4 for OID)
        let msg = vec![b'I', 0, 0];
        let err = parser
            .parse_wal_message(&msg, &catalog)
            .expect_err("should fail");
        assert!(matches!(
            err,
            WalParseError::TruncatedMessage { .. }
        ));
    }

    // -- Test 12: NULL columns ('n' tag) → Cell::Null ------------------------

    #[test]
    fn null_column_tag() {
        let catalog = TestCatalog::orders();
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

    // -- Test 13: Unchanged TOAST ('u' tag) → Cell::Missing ------------------

    #[test]
    fn unchanged_toast_tag() {
        let catalog = TestCatalog::orders();
        let parser = PgOutputParser::new();

        let rel_msg = build_relation_msg(16384, "public", "orders", &orders_columns());
        parser
            .parse_wal_message(&rel_msg, &catalog)
            .expect("relation should parse");

        // Update with 'K' old key, where some columns are unchanged TOAST
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
                TupleCol::Unchanged,
                TupleCol::Text("200.00".into()),
                TupleCol::Text("shipped".into()),
            ],
        );

        let events = parser
            .parse_wal_message(&update_msg, &catalog)
            .expect("update should parse");

        let ev = &events[0];
        let old = ev.old_row.as_ref().expect("should have old_row");
        assert_eq!(old.get(1), Some(&Cell::Missing)); // unchanged TOAST

        let new = ev.new_row.as_ref().expect("should have new_row");
        assert_eq!(new.get(1), Some(&Cell::Missing)); // unchanged TOAST in new too
        assert_eq!(new.get(2), Some(&Cell::Float(200.0)));
    }

    // -- Test 14: Type conversion for various OIDs ---------------------------

    #[test]
    fn type_conversion_oids() {
        let catalog = TestCatalog::orders();
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
                TupleCol::Text("3.14".into()),
                TupleCol::Text("t".into()),
            ],
        );

        let events = parser
            .parse_wal_message(&insert_msg, &catalog)
            .expect("insert should parse");

        let new = events[0].new_row.as_ref().expect("should have new_row");
        assert_eq!(new.get(0), Some(&Cell::Int(42)));
        assert_eq!(new.get(1), Some(&Cell::String(Arc::from("bob"))));
        assert_eq!(new.get(2), Some(&Cell::Float(3.14)));
        assert_eq!(new.get(3), Some(&Cell::Bool(true)));
    }

    // -- Test 15: changed_columns computed correctly for FULL update ----------

    #[test]
    fn changed_columns_full_update() {
        let catalog = TestCatalog::orders();
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
        let catalog = TestCatalog::orders();

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
        let catalog = TestCatalog::orders_no_pk();
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
        let catalog = TestCatalog::orders();
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
        let catalog = TestCatalog::orders();
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
}
