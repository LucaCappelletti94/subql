//! Configuration management for SubQL service

use crate::DurabilityMode;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use thiserror::Error;

/// Configuration errors
#[derive(Error, Debug)]
pub enum ConfigError {
    /// I/O error reading config file
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// TOML deserialize error
    #[error("TOML deserialize error: {0}")]
    Deserialize(#[from] toml::de::Error),

    /// TOML serialize error
    #[error("TOML serialize error: {0}")]
    Serialize(#[from] toml::ser::Error),
}

/// Main configuration structure
#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Storage directory for shards
    pub storage_path: PathBuf,

    /// Shard rotation threshold (bytes)
    #[serde(default = "default_rotation_threshold")]
    pub rotation_threshold: usize,

    /// Merge settings
    #[serde(default)]
    pub merge: MergeConfig,

    /// Durability policy for register/register_batch persistence.
    #[serde(default = "default_durability_mode")]
    pub durability_mode: DurabilityMode,

    /// Schema catalog configuration
    pub catalog: CatalogConfig,
}

/// Merge configuration
#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct MergeConfig {
    /// Auto-merge when shard count exceeds this
    #[serde(default = "default_merge_threshold")]
    pub shard_threshold: usize,

    /// Merge interval (seconds)
    #[serde(default = "default_merge_interval")]
    pub interval_secs: u64,
}

/// Schema catalog configuration
#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct CatalogConfig {
    /// Database URL for schema introspection
    pub database_url: String,

    /// SQL dialect (postgres, mysql, sqlite)
    #[serde(default = "default_dialect")]
    pub dialect: String,
}

// Default values

/// Default shard rotation threshold (10 MB).
///
/// Also used by [`crate::runtime::engine`] for the default `rotation_threshold`
/// on [`SubscriptionEngine`](crate::runtime::engine::SubscriptionEngine) builder paths.
pub const DEFAULT_ROTATION_THRESHOLD: usize = 10 * 1024 * 1024;

const fn default_rotation_threshold() -> usize {
    DEFAULT_ROTATION_THRESHOLD
}

const fn default_merge_threshold() -> usize {
    5
}

const fn default_merge_interval() -> u64 {
    3600 // 1 hour
}

fn default_dialect() -> String {
    "postgres".to_string()
}

const fn default_durability_mode() -> DurabilityMode {
    DurabilityMode::Required
}

impl Config {
    /// Validate configuration values, returning a descriptive error if any
    /// field is out of range or logically inconsistent.
    pub fn validate(&self) -> Result<(), String> {
        if self.rotation_threshold == 0 {
            return Err("rotation_threshold must be greater than 0".to_string());
        }
        if self.merge.shard_threshold == 0 {
            return Err("merge.shard_threshold must be greater than 0".to_string());
        }
        if self.merge.interval_secs == 0 {
            return Err("merge.interval_secs must be greater than 0".to_string());
        }
        if self.catalog.database_url.is_empty() {
            return Err("catalog.database_url must not be empty".to_string());
        }
        Ok(())
    }
}

impl Default for MergeConfig {
    fn default() -> Self {
        Self {
            shard_threshold: default_merge_threshold(),
            interval_secs: default_merge_interval(),
        }
    }
}

/// Load configuration from TOML file
pub fn load_config(path: &Path) -> Result<Config, ConfigError> {
    let contents = std::fs::read_to_string(path)?;
    let config: Config = toml::from_str(&contents)?;
    Ok(config)
}

/// Save configuration to TOML file
pub fn save_config(path: &Path, config: &Config) -> Result<(), ConfigError> {
    let toml_string = toml::to_string_pretty(config)?;
    std::fs::write(path, toml_string)?;
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_config_roundtrip() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("test.toml");

        let config = Config {
            storage_path: PathBuf::from("/var/lib/subql"),
            rotation_threshold: 5 * 1024 * 1024,
            merge: MergeConfig {
                shard_threshold: 10,
                interval_secs: 7200,
            },
            durability_mode: DurabilityMode::Required,
            catalog: CatalogConfig {
                database_url: "postgresql://localhost/test".to_string(),
                dialect: "postgres".to_string(),
            },
        };

        // Save and load
        save_config(&config_path, &config).unwrap();
        let loaded = load_config(&config_path).unwrap();

        assert_eq!(config.storage_path, loaded.storage_path);
        assert_eq!(config.rotation_threshold, loaded.rotation_threshold);
        assert_eq!(config.merge.shard_threshold, loaded.merge.shard_threshold);
        assert_eq!(config.catalog.database_url, loaded.catalog.database_url);
    }

    #[test]
    fn test_default_values() {
        let toml_str = r#"
            storage_path = "/tmp/subql"

            [catalog]
            database_url = "postgresql://localhost/db"
        "#;

        let config: Config = toml::from_str(toml_str).unwrap();

        // Check defaults applied
        assert_eq!(config.rotation_threshold, 10 * 1024 * 1024);
        assert_eq!(config.merge.shard_threshold, 5);
        assert_eq!(config.merge.interval_secs, 3600);
        assert_eq!(config.durability_mode, DurabilityMode::Required);
        assert_eq!(config.catalog.dialect, "postgres");
    }

    #[test]
    fn test_validate_valid_config() {
        let config = Config {
            storage_path: PathBuf::from("/tmp/subql"),
            rotation_threshold: 1024,
            merge: MergeConfig {
                shard_threshold: 5,
                interval_secs: 3600,
            },
            durability_mode: DurabilityMode::Required,
            catalog: CatalogConfig {
                database_url: "postgresql://localhost/db".to_string(),
                dialect: "postgres".to_string(),
            },
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_zero_rotation_threshold() {
        let config = Config {
            storage_path: PathBuf::from("/tmp/subql"),
            rotation_threshold: 0,
            merge: MergeConfig {
                shard_threshold: 5,
                interval_secs: 3600,
            },
            durability_mode: DurabilityMode::Required,
            catalog: CatalogConfig {
                database_url: "postgresql://localhost/db".to_string(),
                dialect: "postgres".to_string(),
            },
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_empty_database_url() {
        let config = Config {
            storage_path: PathBuf::from("/tmp/subql"),
            rotation_threshold: 1024,
            merge: MergeConfig {
                shard_threshold: 5,
                interval_secs: 3600,
            },
            durability_mode: DurabilityMode::Required,
            catalog: CatalogConfig {
                database_url: String::new(),
                dialect: "postgres".to_string(),
            },
        };
        assert!(config.validate().is_err());
    }
}
