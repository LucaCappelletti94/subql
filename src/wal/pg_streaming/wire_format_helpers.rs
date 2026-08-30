use alloc::string::{String, ToString};

pub(super) const XLOG_DATA_HEADER_LEN: usize = 1 + 8 + 8 + 8; // 'w' + start + end + clock
pub(super) const PRIMARY_KEEPALIVE_LEN: usize = 1 + 8 + 8 + 1; // 'k' + end + clock + reply

/// Append `replication=database` to a libpq conninfo string if the
/// caller did not already include it. Accepts both URL-style
/// (`postgresql://...`) and key=value forms. The latter just needs the
/// param appended.
pub(super) fn ensure_replication_param(url: &str) -> String {
    if url.contains("replication=") {
        return url.to_string();
    }
    if url.contains("://") {
        if url.contains('?') {
            alloc::format!("{url}&replication=database")
        } else {
            alloc::format!("{url}?replication=database")
        }
    } else {
        alloc::format!("{url} replication=database")
    }
}

#[cfg(test)]
mod tests {
    use super::{ensure_replication_param, PRIMARY_KEEPALIVE_LEN, XLOG_DATA_HEADER_LEN};

    #[test]
    fn xlog_data_header_constants_match_spec() {
        assert_eq!(XLOG_DATA_HEADER_LEN, 25);
        assert_eq!(PRIMARY_KEEPALIVE_LEN, 18);
    }

    #[test]
    fn ensure_replication_param_url_no_query() {
        assert_eq!(
            ensure_replication_param("postgresql://u:p@h:5432/db"),
            "postgresql://u:p@h:5432/db?replication=database"
        );
    }

    #[test]
    fn ensure_replication_param_url_with_query() {
        assert_eq!(
            ensure_replication_param("postgresql://u:p@h:5432/db?sslmode=require"),
            "postgresql://u:p@h:5432/db?sslmode=require&replication=database"
        );
    }

    #[test]
    fn ensure_replication_param_keyvalue_form() {
        assert_eq!(
            ensure_replication_param("host=h port=5432 dbname=db"),
            "host=h port=5432 dbname=db replication=database"
        );
    }

    #[test]
    fn ensure_replication_param_already_present_is_idempotent() {
        let s = "postgresql://u:p@h:5432/db?replication=database";
        assert_eq!(ensure_replication_param(s), s);
    }
}
