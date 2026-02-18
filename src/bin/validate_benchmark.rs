//! Validate SubQL against real-world SQL queries from sql_ast_benchmark
//!
//! Tests the engine against 6,400+ real-world SQL queries to identify:
//! - Parsing success rate
//! - Compilation success rate
//! - Unsupported SQL patterns
//! - Common failure modes

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::sync::Arc;
use std::collections::HashMap;
use subql::{SubscriptionEngine, SubscriptionSpec, SchemaCatalog, TableId};
use sqlparser::dialect::PostgreSqlDialect;

/// Mock catalog that accepts any table/column
struct PermissiveCatalog;

impl SchemaCatalog for PermissiveCatalog {
    fn table_id(&self, _table_name: &str) -> Option<TableId> {
        Some(1) // Accept any table
    }

    fn column_id(&self, _table_id: TableId, _column_name: &str) -> Option<u16> {
        Some(0) // Accept any column
    }

    fn table_arity(&self, _table_id: TableId) -> Option<usize> {
        Some(100) // Generous arity
    }

    fn schema_fingerprint(&self, _table_id: TableId) -> Option<u64> {
        Some(0x1234567890ABCDEF)
    }
}

#[derive(Default)]
struct ValidationStats {
    total: usize,
    parsed: usize,
    compiled: usize,
    registered: usize,
    parse_errors: HashMap<String, usize>,
    compile_errors: HashMap<String, usize>,
    register_errors: HashMap<String, usize>,
}

impl ValidationStats {
    fn record_parse_error(&mut self, error: &str) {
        let error_type = extract_error_type(error);
        *self.parse_errors.entry(error_type).or_insert(0) += 1;
    }

    fn record_compile_error(&mut self, error: &str) {
        let error_type = extract_error_type(error);
        *self.compile_errors.entry(error_type).or_insert(0) += 1;
    }

    fn record_register_error(&mut self, error: &str) {
        let error_type = extract_error_type(error);
        *self.register_errors.entry(error_type).or_insert(0) += 1;
    }

    fn print_summary(&self) {
        println!("\n╔══════════════════════════════════════════════════════════════╗");
        println!("║           SQL Benchmark Validation Results                  ║");
        println!("╚══════════════════════════════════════════════════════════════╝\n");

        println!("📊 Overall Statistics");
        println!("═══════════════════════════════════════════════════════════════");
        println!("  Total queries tested:     {}", self.total);
        println!("  Successfully parsed:      {} ({:.1}%)", self.parsed,
                 self.parsed as f64 / self.total as f64 * 100.0);
        println!("  Successfully compiled:    {} ({:.1}%)", self.compiled,
                 self.compiled as f64 / self.total as f64 * 100.0);
        println!("  Successfully registered:  {} ({:.1}%)", self.registered,
                 self.registered as f64 / self.total as f64 * 100.0);
        println!();

        if !self.parse_errors.is_empty() {
            println!("❌ Parse Errors (Top 10)");
            println!("═══════════════════════════════════════════════════════════════");
            let mut errors: Vec<_> = self.parse_errors.iter().collect();
            errors.sort_by_key(|(_, count)| std::cmp::Reverse(**count));
            for (error, count) in errors.iter().take(10) {
                println!("  [{:4}] {}", count, error);
            }
            println!();
        }

        if !self.compile_errors.is_empty() {
            println!("⚠️  Compile Errors (Top 10)");
            println!("═══════════════════════════════════════════════════════════════");
            let mut errors: Vec<_> = self.compile_errors.iter().collect();
            errors.sort_by_key(|(_, count)| std::cmp::Reverse(**count));
            for (error, count) in errors.iter().take(10) {
                println!("  [{:4}] {}", count, error);
            }
            println!();
        }

        if !self.register_errors.is_empty() {
            println!("⚠️  Registration Errors (Top 10)");
            println!("═══════════════════════════════════════════════════════════════");
            let mut errors: Vec<_> = self.register_errors.iter().collect();
            errors.sort_by_key(|(_, count)| std::cmp::Reverse(**count));
            for (error, count) in errors.iter().take(10) {
                println!("  [{:4}] {}", count, error);
            }
            println!();
        }

        println!("✅ Success Rate: {:.1}%",
                 self.registered as f64 / self.total as f64 * 100.0);
    }
}

fn extract_error_type(error: &str) -> String {
    // Extract the main error type, ignoring specific details
    if error.contains("Unsupported SQL") {
        "Unsupported SQL feature".to_string()
    } else if error.contains("Unknown table") {
        "Unknown table".to_string()
    } else if error.contains("Unknown column") {
        "Unknown column".to_string()
    } else if error.contains("parse error") || error.contains("Parse error") {
        "SQL parse error".to_string()
    } else if error.contains("Type error") {
        "Type error".to_string()
    } else {
        // Take first 50 chars as error type
        error.chars().take(50).collect()
    }
}

fn validate_sql_file(path: &str, stats: &mut ValidationStats, sample_failures: &mut Vec<String>) {
    let file = match File::open(path) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("Failed to open {}: {}", path, e);
            return;
        }
    };

    let reader = BufReader::new(file);
    let catalog = Arc::new(PermissiveCatalog);
    let mut engine = SubscriptionEngine::new(catalog, PostgreSqlDialect {});

    for (line_num, line) in reader.lines().enumerate() {
        let sql = match line {
            Ok(l) => l.trim().to_string(),
            Err(_) => continue,
        };

        if sql.is_empty() || sql.starts_with("--") {
            continue;
        }

        stats.total += 1;

        // Try to register as subscription
        let spec = SubscriptionSpec {
            subscription_id: stats.total as u64,
            user_id: 1,
            session_id: None,
            sql: sql.clone(),
            updated_at_unix_ms: 0,
        };

        match engine.register(spec) {
            Ok(_) => {
                stats.parsed += 1;
                stats.compiled += 1;
                stats.registered += 1;
            }
            Err(e) => {
                let error_str = e.to_string();

                if error_str.contains("parse error") || error_str.contains("Parse error") {
                    stats.record_parse_error(&error_str);
                } else if error_str.contains("Unsupported SQL") {
                    stats.parsed += 1; // Parsed successfully
                    stats.record_compile_error(&error_str);
                } else {
                    stats.parsed += 1;
                    stats.compiled += 1;
                    stats.record_register_error(&error_str);
                }

                // Collect sample failures
                if sample_failures.len() < 10 {
                    sample_failures.push(format!("Line {}: {} - Error: {}",
                        line_num + 1, sql, error_str));
                }
            }
        }

        // Progress indicator
        if stats.total % 500 == 0 {
            print!(".");
            std::io::Write::flush(&mut std::io::stdout()).ok();
        }
    }
}

fn main() {
    println!("SubQL Benchmark Validation");
    println!("Testing against real-world SQL queries from sql_ast_benchmark");
    println!();

    let mut stats = ValidationStats::default();
    let mut sample_failures = Vec::new();

    let datasets = vec![
        ("/tmp/sql_ast_benchmark/gretel_select.txt", "Gretel SELECT"),
        ("/tmp/sql_ast_benchmark/spider_select.txt", "Spider SELECT"),
    ];

    for (path, name) in datasets {
        println!("Testing {} queries...", name);
        validate_sql_file(path, &mut stats, &mut sample_failures);
        println!(" done!");
    }

    stats.print_summary();

    if !sample_failures.is_empty() {
        println!("\n📋 Sample Failures (first 10)");
        println!("═══════════════════════════════════════════════════════════════");
        for failure in sample_failures.iter().take(10) {
            println!("  {}", failure);
        }
    }

    println!();
    println!("Validation complete! 🎉");
}
