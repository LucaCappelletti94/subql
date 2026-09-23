use super::{PgStreamingError, TimelineSwitch};
use crate::PgLsn;
use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

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

/// Parse one timeline history file, `parent_tli <TAB> switchpoint <TAB> reason` per entry with blank lines between entries, failing on any malformed line rather than dropping it.
pub(super) fn parse_timeline_history(
    content: &str,
) -> Result<Vec<TimelineSwitch>, PgStreamingError> {
    let mut switches = Vec::new();
    for line in content.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let malformed = || {
            PgStreamingError::Protocol(format!(
                "malformed TIMELINE_HISTORY entry {line:?}, expected parent_tli, \
                 switch point and reason tab-separated"
            ))
        };
        let mut fields = line.split('\t');
        let timeline = fields
            .next()
            .and_then(|field| field.trim().parse::<u32>().ok())
            .ok_or_else(malformed)?;
        let switch_lsn = fields
            .next()
            .and_then(|field| PgLsn::parse(field.trim()))
            .ok_or_else(malformed)?;
        if fields.next().is_none() {
            return Err(malformed());
        }
        switches.push(TimelineSwitch {
            timeline,
            switch_lsn,
        });
    }
    Ok(switches)
}

#[cfg(test)]
mod tests {
    use super::super::{PgStreamingError, TimelineSwitch};
    use super::parse_timeline_history;
    use super::{ensure_replication_param, PRIMARY_KEEPALIVE_LEN, XLOG_DATA_HEADER_LEN};
    use crate::PgLsn;
    use alloc::vec;

    /// The bytes of a real `00000003.history`, including the blank line between entries.
    #[test]
    fn parses_a_two_ancestor_history_with_blank_line_separators() {
        let content = "1\t0/2000100\treached consistency\n\n2\t0/3000100\treached consistency\n";
        let switches = parse_timeline_history(content).expect("file shape parses");
        assert_eq!(
            switches,
            vec![
                TimelineSwitch {
                    timeline: 1,
                    switch_lsn: PgLsn(0x0200_0100),
                },
                TimelineSwitch {
                    timeline: 2,
                    switch_lsn: PgLsn(0x0300_0100),
                },
            ]
        );
    }

    #[test]
    fn empty_content_is_an_empty_history() {
        let empty: Vec<TimelineSwitch> = Vec::new();
        assert_eq!(parse_timeline_history("").expect("empty parses"), empty);
        assert_eq!(parse_timeline_history("\n\n").expect("blanks parse"), empty);
    }

    #[test]
    fn a_malformed_line_names_the_line_and_fails() {
        for bad in [
            "1 0/2000100 reason\n",
            "x\t0/2000100\treached consistency\n",
            "1\tnotanlsn\treached consistency\n",
            "1\t0/2000100\n",
            "1\t/2000100\treached consistency\n",
        ] {
            let err = parse_timeline_history(bad).expect_err("must not silently pass");
            match err {
                PgStreamingError::Protocol(msg) => {
                    let named = format!("{:?}", bad.trim_end());
                    assert!(msg.contains(&named), "message {msg:?} must name {named}");
                }
                other => panic!("expected Protocol, got {other:?}"),
            }
        }
    }

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
