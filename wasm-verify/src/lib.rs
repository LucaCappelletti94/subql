//! Isolated headless-wasm verification that `SqliteCdcSource` works in the
//! browser: build a source over an in-browser sqlite connection, run DML, and
//! drain real `WalEvent`s. No Dioxus, so this isolates the source.

// Ensure the browser sqlite shim is linked so diesel finds libsqlite3 symbols.
#[cfg(target_arch = "wasm32")]
use sqlite_wasm_rs as _;

#[cfg(all(test, target_arch = "wasm32"))]
mod tests {
    use diesel::{sqlite::SqliteConnection, Connection};
    use subql::{SqliteCdcConfig, SqliteCdcSource};
    use wasm_bindgen_test::*;

    wasm_bindgen_test_configure!(run_in_browser);

    const DDL: &str = "CREATE TABLE orders (id INT PRIMARY KEY, amount INT, status TEXT);";

    #[wasm_bindgen_test]
    fn insert_update_delete_drain_events() {
        let conn = SqliteConnection::establish(":memory:").expect("establish in-memory sqlite");
        let mut src = SqliteCdcSource::with_pg_ddl(conn, DDL, SqliteCdcConfig::default())
            .expect("build SqliteCdcSource");

        let n = src
            .execute("INSERT INTO orders (id, amount, status) VALUES (1, 250, 'paid')")
            .expect("execute insert");
        assert_eq!(n, 1, "one row inserted");

        let ev = src
            .poll_next_event()
            .expect("poll ok")
            .expect("an insert event was produced");
        assert!(
            format!("{ev:?}").to_lowercase().contains("insert"),
            "expected an insert WalEvent, got {ev:?}"
        );

        src.execute("UPDATE orders SET amount = 300 WHERE id = 1")
            .expect("execute update");
        let ev = src
            .poll_next_event()
            .expect("poll ok")
            .expect("update event");
        assert!(
            format!("{ev:?}").to_lowercase().contains("update"),
            "got {ev:?}"
        );

        src.execute("DELETE FROM orders WHERE id = 1")
            .expect("execute delete");
        let ev = src
            .poll_next_event()
            .expect("poll ok")
            .expect("delete event");
        assert!(
            format!("{ev:?}").to_lowercase().contains("delete"),
            "got {ev:?}"
        );
    }
}
