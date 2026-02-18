// Enhanced analysis to find edge cases and "Other" failures
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::sync::Arc;
use std::collections::HashMap;
use subql::{SubscriptionEngine, SubscriptionSpec, SchemaCatalog, TableId};
use sqlparser::dialect::PostgreSqlDialect;

struct PermissiveCatalog;

impl SchemaCatalog for PermissiveCatalog {
    fn table_id(&self, _: &str) -> Option<TableId> { Some(1) }
    fn column_id(&self, _: TableId, _: &str) -> Option<u16> { Some(0) }
    fn table_arity(&self, _: TableId) -> Option<usize> { Some(100) }
    fn schema_fingerprint(&self, _: TableId) -> Option<u64> { Some(0x1234) }
}

#[derive(Default)]
struct FailureAnalysis {
    joins: usize,
    subqueries: usize,
    window_functions: usize,
    group_by: usize,
    aggregates: usize,
    other: Vec<(String, String)>, // (SQL, Error)
}

fn categorize_error(sql: &str, error: &str) -> &'static str {
    let sql_lower = sql.to_lowercase();
    let error_lower = error.to_lowercase();

    if error_lower.contains("join") {
        "JOIN"
    } else if error_lower.contains("subquery") || error_lower.contains("insubquery") {
        "Subquery/IN(SELECT)"
    } else if sql_lower.contains("over (") || sql_lower.contains("over(") {
        "Window function"
    } else if error_lower.contains("derived") || sql_lower.contains("from (select") {
        "Derived table"
    } else if error_lower.contains("group by") || error_lower.contains("group_by") {
        "GROUP BY"
    } else if error_lower.contains("aggregate") {
        "Aggregate without WHERE"
    } else {
        "Other"
    }
}

fn analyze_file(path: &str) -> FailureAnalysis {
    let catalog = Arc::new(PermissiveCatalog);
    let mut engine = SubscriptionEngine::new(catalog, PostgreSqlDialect {});
    let mut analysis = FailureAnalysis::default();

    let file = File::open(path).unwrap();
    let reader = BufReader::new(file);

    for (idx, line) in reader.lines().enumerate() {
        let sql = line.unwrap().trim().to_string();
        if sql.is_empty() { continue; }

        let spec = SubscriptionSpec {
            subscription_id: idx as u64,
            user_id: 1,
            session_id: None,
            sql: sql.clone(),
            updated_at_unix_ms: 0,
        };

        if let Err(e) = engine.register(spec) {
            let error_str = e.to_string();
            let category = categorize_error(&sql, &error_str);

            match category {
                "JOIN" => analysis.joins += 1,
                "Subquery/IN(SELECT)" => analysis.subqueries += 1,
                "Window function" => analysis.window_functions += 1,
                "GROUP BY" => analysis.group_by += 1,
                "Aggregate without WHERE" => analysis.aggregates += 1,
                "Other" => {
                    // Collect ALL "Other" failures
                    analysis.other.push((sql.clone(), error_str.clone()));
                }
                _ => {}
            }
        }
    }

    analysis
}

fn main() {
    println!("\n╔══════════════════════════════════════════════════════════════╗");
    println!("║           Edge Case Analysis - Both Datasets                ║");
    println!("╚══════════════════════════════════════════════════════════════╝\n");

    // Analyze both datasets
    println!("Analyzing Gretel dataset...");
    let gretel = analyze_file("/tmp/sql_ast_benchmark/gretel_select.txt");

    println!("Analyzing Spider dataset...");
    let spider = analyze_file("/tmp/sql_ast_benchmark/spider_select.txt");

    println!("\n📊 Summary of Failures:");
    println!("═══════════════════════════════════════════════════════════════\n");

    println!("  Category                  Gretel      Spider      Total");
    println!("  ──────────────────────────────────────────────────────────");
    println!("  JOINs                     {:6}      {:6}      {:6}",
        gretel.joins, spider.joins, gretel.joins + spider.joins);
    println!("  Subqueries                {:6}      {:6}      {:6}",
        gretel.subqueries, spider.subqueries, gretel.subqueries + spider.subqueries);
    println!("  Window functions          {:6}      {:6}      {:6}",
        gretel.window_functions, spider.window_functions,
        gretel.window_functions + spider.window_functions);
    println!("  GROUP BY                  {:6}      {:6}      {:6}",
        gretel.group_by, spider.group_by, gretel.group_by + spider.group_by);
    println!("  Aggregates                {:6}      {:6}      {:6}",
        gretel.aggregates, spider.aggregates, gretel.aggregates + spider.aggregates);
    println!("  OTHER (edge cases)        {:6}      {:6}      {:6}",
        gretel.other.len(), spider.other.len(), gretel.other.len() + spider.other.len());

    // Detailed analysis of "Other" failures
    println!("\n\n🔍 EDGE CASES - \"Other\" Failures:");
    println!("═══════════════════════════════════════════════════════════════\n");

    println!("Gretel Dataset ({} edge cases):", gretel.other.len());
    println!("───────────────────────────────────────────────────────────────");

    // Group by error message
    let mut gretel_by_error: HashMap<String, Vec<String>> = HashMap::new();
    for (sql, error) in &gretel.other {
        gretel_by_error.entry(error.clone()).or_default().push(sql.clone());
    }

    for (error, sqls) in gretel_by_error.iter() {
        println!("\n  Error: {}", error);
        println!("  Count: {}", sqls.len());
        println!("  Examples:");
        for (i, sql) in sqls.iter().take(3).enumerate() {
            println!("    {}. {}", i + 1, sql);
        }
    }

    println!("\n\nSpider Dataset ({} edge cases):", spider.other.len());
    println!("───────────────────────────────────────────────────────────────");

    let mut spider_by_error: HashMap<String, Vec<String>> = HashMap::new();
    for (sql, error) in &spider.other {
        spider_by_error.entry(error.clone()).or_default().push(sql.clone());
    }

    for (error, sqls) in spider_by_error.iter() {
        println!("\n  Error: {}", error);
        println!("  Count: {}", sqls.len());
        println!("  Examples:");
        for (i, sql) in sqls.iter().take(3).enumerate() {
            println!("    {}. {}", i + 1, sql);
        }
    }

    println!("\n\n📝 Summary:");
    println!("═══════════════════════════════════════════════════════════════");
    println!("  Total \"Other\" edge cases: {}", gretel.other.len() + spider.other.len());
    println!("  Unique error patterns (Gretel): {}", gretel_by_error.len());
    println!("  Unique error patterns (Spider): {}", spider_by_error.len());
}
