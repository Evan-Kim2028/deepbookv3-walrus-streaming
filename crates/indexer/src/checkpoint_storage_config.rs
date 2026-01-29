// Copyright (c) DeepBook V3. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use clap::{Parser, ValueEnum};
use std::path::PathBuf;

/// Checkpoint storage backend selection
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum CheckpointStorageType {
    /// Sui's official checkpoint bucket (sequential downloads)
    #[clap(name = "sui")]
    Sui,

    /// Walrus aggregator with blob-based storage (fast backfill)
    #[clap(name = "walrus")]
    Walrus,
}

impl std::fmt::Display for CheckpointStorageType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Sui => write!(f, "sui"),
            Self::Walrus => write!(f, "walrus"),
        }
    }
}

impl std::str::FromStr for CheckpointStorageType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "sui" => Ok(CheckpointStorageType::Sui),
            "walrus" => Ok(CheckpointStorageType::Walrus),
            _ => Err(format!("invalid checkpoint storage: {}", s)),
        }
    }
}

/// Checkpoint storage configuration
#[derive(Debug, Clone, Parser)]
pub struct CheckpointStorageConfig {
    /// Which checkpoint storage backend to use
    #[arg(long, env = "CHECKPOINT_STORAGE", default_value = "sui")]
    pub storage: CheckpointStorageType,

    /// Walrus archival service URL (for blob metadata)
    #[arg(long, env = "WALRUS_ARCHIVAL_URL", default_value = "https://walrus-sui-archival.mainnet.walrus.space")]
    pub walrus_archival_url: String,

    /// Walrus aggregator URL (for blob downloads)
    #[arg(long, env = "WALRUS_AGGREGATOR_URL", default_value = "https://aggregator.walrus-mainnet.walrus.space")]
    pub walrus_aggregator_url: String,

    /// Enable local blob caching (highly recommended)
    #[arg(long, env = "CHECKPOINT_CACHE_ENABLED", default_value = "true")]
    pub cache_enabled: bool,

    /// Directory for checkpoint blob cache
    #[arg(long, env = "CHECKPOINT_CACHE_DIR", default_value = "./checkpoint_cache")]
    pub cache_dir: PathBuf,

    /// Maximum cache size in GB (0 = unlimited)
    #[arg(long, env = "CHECKPOINT_CACHE_MAX_SIZE_GB", default_value = "100")]
    pub cache_max_size_gb: u64,

    /// Path to the Walrus CLI binary (optional, used if aggregator is skipped)
    #[arg(long, env = "WALRUS_CLI_PATH")]
    pub walrus_cli_path: Option<PathBuf>,

    /// Walrus CLI context (default: mainnet)
    #[arg(long, env = "WALRUS_CLI_CONTEXT", default_value = "mainnet")]
    pub walrus_cli_context: String,

    /// Skip Walrus CLI consistency checks (faster, only for trusted data)
    #[arg(long, env = "WALRUS_CLI_SKIP_CONSISTENCY_CHECK", default_value = "false")]
    pub walrus_cli_skip_consistency_check: bool,

    /// Walrus CLI command timeout in seconds
    #[arg(long, env = "WALRUS_CLI_TIMEOUT_SECS", default_value = "120")]
    pub walrus_cli_timeout_secs: u64,

    /// Use walrus CLI size-only fallback when footer magic is invalid
    #[arg(long, env = "WALRUS_CLI_SIZE_ONLY_FALLBACK", default_value = "false")]
    pub walrus_cli_size_only_fallback: bool,

    /// HTTP timeout (seconds) for Walrus aggregator requests
    #[arg(long, env = "WALRUS_HTTP_TIMEOUT_SECS", default_value = "60")]
    pub walrus_http_timeout_secs: u64,

    /// Range coalescing gap threshold in bytes (0 = only coalesce contiguous ranges)
    #[arg(long, env = "WALRUS_COALESCE_GAP_BYTES", default_value = "0")]
    pub walrus_coalesce_gap_bytes: u64,

    /// Max coalesced range size in bytes when using HTTP aggregator
    #[arg(long, env = "WALRUS_COALESCE_MAX_RANGE_BYTES", default_value = "33554432")]
    pub walrus_coalesce_max_range_bytes: u64,

    /// Max coalesced range size in bytes when using Walrus CLI/local cache
    #[arg(long, env = "WALRUS_COALESCE_MAX_RANGE_BYTES_CLI", default_value = "33554432")]
    pub walrus_coalesce_max_range_bytes_cli: u64,

    /// Max concurrent blobs to process when using Walrus CLI
    #[arg(long, env = "WALRUS_CLI_BLOB_CONCURRENCY", default_value = "2")]
    pub walrus_cli_blob_concurrency: usize,

    /// Max concurrent range reads per blob when using Walrus CLI
    #[arg(long, env = "WALRUS_CLI_RANGE_CONCURRENCY", default_value = "2")]
    pub walrus_cli_range_concurrency: usize,

    /// Max retries for Walrus CLI range reads (>1MB)
    #[arg(long, env = "WALRUS_CLI_RANGE_MAX_RETRIES", default_value = "6")]
    pub walrus_cli_range_max_retries: usize,

    /// Max retries for small Walrus CLI range reads (<=1MB)
    #[arg(long, env = "WALRUS_CLI_RANGE_SMALL_MAX_RETRIES", default_value = "3")]
    pub walrus_cli_range_small_max_retries: usize,

    /// Delay between Walrus CLI range retries (seconds)
    #[arg(long, env = "WALRUS_CLI_RANGE_RETRY_DELAY_SECS", default_value = "5")]
    pub walrus_cli_range_retry_delay_secs: u64,

    /// Minimum range size (bytes) before splitting on repeated failures (0 disables splitting)
    #[arg(long, env = "WALRUS_CLI_RANGE_MIN_SPLIT_BYTES", default_value = "4194304")]
    pub walrus_cli_range_min_split_bytes: u64,
}
