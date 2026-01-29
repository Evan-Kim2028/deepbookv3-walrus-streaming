use anyhow::Context;
use clap::Parser;
use deepbook_indexer::handlers::balances_handler::BalancesHandler;
use deepbook_indexer::handlers::deep_burned_handler::DeepBurnedHandler;
use deepbook_indexer::handlers::flash_loan_handler::FlashLoanHandler;
use deepbook_indexer::handlers::order_fill_handler::OrderFillHandler;
use deepbook_indexer::handlers::order_update_handler::OrderUpdateHandler;
use deepbook_indexer::handlers::pool_price_handler::PoolPriceHandler;
use deepbook_indexer::handlers::proposals_handler::ProposalsHandler;
use deepbook_indexer::handlers::rebates_handler::RebatesHandler;
use deepbook_indexer::handlers::referral_fee_event_handler::ReferralFeeEventHandler;
use deepbook_indexer::handlers::stakes_handler::StakesHandler;
use deepbook_indexer::handlers::trade_params_update_handler::TradeParamsUpdateHandler;
use deepbook_indexer::handlers::vote_handler::VotesHandler;

// Margin Manager Events
use deepbook_indexer::handlers::liquidation_handler::LiquidationHandler;
use deepbook_indexer::handlers::loan_borrowed_handler::LoanBorrowedHandler;
use deepbook_indexer::handlers::loan_repaid_handler::LoanRepaidHandler;
use deepbook_indexer::handlers::margin_manager_created_handler::MarginManagerCreatedHandler;

// Margin Pool Operations Events
use deepbook_indexer::handlers::asset_supplied_handler::AssetSuppliedHandler;
use deepbook_indexer::handlers::asset_withdrawn_handler::AssetWithdrawnHandler;
use deepbook_indexer::handlers::maintainer_fees_withdrawn_handler::MaintainerFeesWithdrawnHandler;
use deepbook_indexer::handlers::protocol_fees_withdrawn_handler::ProtocolFeesWithdrawnHandler;
use deepbook_indexer::handlers::supplier_cap_minted_handler::SupplierCapMintedHandler;
use deepbook_indexer::handlers::supply_referral_minted_handler::SupplyReferralMintedHandler;

// Margin Pool Admin Events
use deepbook_indexer::handlers::deepbook_pool_updated_handler::DeepbookPoolUpdatedHandler;
use deepbook_indexer::handlers::interest_params_updated_handler::InterestParamsUpdatedHandler;
use deepbook_indexer::handlers::margin_pool_config_updated_handler::MarginPoolConfigUpdatedHandler;
use deepbook_indexer::handlers::margin_pool_created_handler::MarginPoolCreatedHandler;

// Margin Registry Events
use deepbook_indexer::handlers::deepbook_pool_config_updated_handler::DeepbookPoolConfigUpdatedHandler;
use deepbook_indexer::handlers::deepbook_pool_registered_handler::DeepbookPoolRegisteredHandler;
use deepbook_indexer::handlers::deepbook_pool_updated_registry_handler::DeepbookPoolUpdatedRegistryHandler;
use deepbook_indexer::handlers::maintainer_cap_updated_handler::MaintainerCapUpdatedHandler;
use deepbook_indexer::handlers::pause_cap_updated_handler::PauseCapUpdatedHandler;

// Protocol Fees Events
use deepbook_indexer::handlers::protocol_fees_increased_handler::ProtocolFeesIncreasedHandler;
use deepbook_indexer::handlers::referral_fees_claimed_handler::ReferralFeesClaimedHandler;

// Collateral Events
use deepbook_indexer::handlers::deposit_collateral_handler::DepositCollateralHandler;
use deepbook_indexer::handlers::withdraw_collateral_handler::WithdrawCollateralHandler;

// TPSL (Take Profit / Stop Loss) Events
use deepbook_indexer::handlers::conditional_order_added_handler::ConditionalOrderAddedHandler;
use deepbook_indexer::handlers::conditional_order_cancelled_handler::ConditionalOrderCancelledHandler;
use deepbook_indexer::handlers::conditional_order_executed_handler::ConditionalOrderExecutedHandler;
use deepbook_indexer::handlers::conditional_order_insufficient_funds_handler::ConditionalOrderInsufficientFundsHandler;

use deepbook_indexer::{
    CheckpointStorage, CheckpointStorageConfig, CheckpointStorageType, DeepbookEnv,
    SuiCheckpointStorage, WalrusCheckpointStorage,
};
use deepbook_schema::MIGRATIONS;
use prometheus::Registry;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use sui_indexer_alt_framework::ingestion::ingestion_client::IngestionClientArgs;
use sui_indexer_alt_framework::ingestion::{ClientArgs, IngestionConfig};
use sui_indexer_alt_framework::pipeline::concurrent::Handler;
use sui_indexer_alt_framework::pipeline::Processor;
use sui_indexer_alt_framework::types::full_checkpoint_content::Checkpoint;
use sui_indexer_alt_framework::{Indexer, IndexerArgs};
use sui_indexer_alt_metrics::db::DbConnectionStatsCollector;
use sui_indexer_alt_metrics::{MetricsArgs, MetricsService};
use sui_pg_db::{Db, DbArgs};
use sui_types::base_types::ObjectID;

use url::Url;

#[derive(Debug, Clone, clap::ValueEnum)]
pub enum Package {
    /// Index DeepBook core events (order fills, updates, pools, etc.)
    Deepbook,
    /// Index DeepBook margin events (lending, borrowing, liquidations, etc.)
    DeepbookMargin,
}

#[derive(Parser)]
#[clap(rename_all = "kebab-case", author, version)]
struct Args {
    #[command(flatten)]
    db_args: DbArgs,
    #[command(flatten)]
    indexer_args: IndexerArgs,
    #[clap(env, long, default_value = "0.0.0.0:9184")]
    metrics_address: SocketAddr,
    #[clap(
        env,
        long,
        default_value = "postgres://postgres:postgrespw@localhost:5432/deepbook"
    )]
    database_url: Url,
    /// Deepbook environment, defaulted to SUI mainnet.
    #[clap(env, long)]
    env: DeepbookEnv,
    /// Packages to index events for (can specify multiple)
    #[clap(long, value_enum, default_values = ["deepbook", "deepbook-margin"])]
    packages: Vec<Package>,

    /// Checkpoint storage configuration
    #[command(flatten)]
    storage_config: CheckpointStorageConfig,

    /// Run a Walrus backfill verification test and exit
    #[clap(long)]
    verify_walrus_backfill: bool,

    /// Start checkpoint for verification (defaults to 238300000)
    #[clap(long)]
    verification_start: Option<u64>,

    /// Number of checkpoints to verify (defaults to 1000)
    #[clap(long)]
    verification_limit: Option<u64>,

    /// Start verification from the latest available checkpoint
    #[clap(long)]
    verification_from_latest: bool,

    /// Download Walrus checkpoints to a local directory (for ingestion)
    #[clap(long)]
    download_walrus_to: Option<PathBuf>,

    /// Build a package checkpoint index from Walrus and exit
    #[clap(long)]
    build_walrus_package_index: bool,

    /// Package ID to scan for (0x...)
    #[clap(long)]
    walrus_package_id: Option<String>,

    /// Start checkpoint for package index scan (defaults to 0)
    #[clap(long)]
    package_index_start: Option<u64>,

    /// Number of checkpoints to scan for package index (defaults to 10000)
    #[clap(long)]
    package_index_limit: Option<u64>,

    /// Start package index scan from the latest available checkpoint
    #[clap(long)]
    package_index_from_latest: bool,

    /// Output path for package index JSON (optional)
    #[clap(long)]
    package_index_out: Option<PathBuf>,

    /// Path to a local directory for ingestion (overrides remote store)
    #[clap(long)]
    local_ingestion_path: Option<PathBuf>,

    /// Run a Walrus CLI-only backfill (bypass Sui checkpoint bucket ingestion)
    #[clap(long)]
    walrus_cli_backfill: bool,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let _guard = telemetry_subscribers::TelemetryConfig::new()
        .with_env()
        .init();

    let Args {
        db_args,
        indexer_args,
        metrics_address,
        database_url,
        env,
        packages,
        storage_config,
        verify_walrus_backfill,
        verification_start,
        verification_limit,
        verification_from_latest,
        download_walrus_to,
        build_walrus_package_index,
        walrus_package_id,
        package_index_start,
        package_index_limit,
        package_index_from_latest,
        package_index_out,
        local_ingestion_path,
        walrus_cli_backfill,
    } = Args::parse();

    if let Some(output_dir) = download_walrus_to {
        tracing::info!("Starting Walrus checkpoint download to {}...", output_dir.display());
        return run_walrus_download(storage_config, verification_start, verification_limit, output_dir).await;
    }

    if verify_walrus_backfill {
        tracing::info!("Starting Walrus backfill verification...");
        return run_walrus_verification(
            storage_config,
            verification_start,
            verification_limit,
            verification_from_latest,
        )
        .await;
    }

    if build_walrus_package_index {
        tracing::info!("Starting Walrus package index scan...");
        return run_walrus_package_index(
            storage_config,
            walrus_package_id,
            package_index_start,
            package_index_limit,
            package_index_from_latest,
            package_index_out,
        )
        .await;
    }

    if walrus_cli_backfill {
        tracing::info!("Starting Walrus CLI-only backfill...");
        return run_walrus_cli_backfill(
            storage_config,
            env,
            packages,
            db_args,
            database_url,
            indexer_args,
        )
        .await;
    }

    let registry = Registry::new_custom(Some("deepbook".into()), None)
        .context("Failed to create Prometheus registry.")?;
    let metrics = MetricsService::new(MetricsArgs { metrics_address }, registry.clone());

    // Prepare the store for the indexer
    let store = Db::for_write(database_url, db_args)
        .await
        .context("Failed to connect to database")?;

    store
        .run_migrations(Some(&MIGRATIONS))
        .await
        .context("Failed to run pending migrations")?;

    registry.register(Box::new(DbConnectionStatsCollector::new(
        Some("deepbook_indexer_db"),
        store.clone(),
    )))?;

    let mut indexer = Indexer::new(
        store,
        indexer_args,
        ClientArgs {
            ingestion: IngestionClientArgs {
                remote_store_url: if local_ingestion_path.is_some() { None } else { Some(env.remote_store_url()) },
                local_ingestion_path: local_ingestion_path.clone(),
                rpc_api_url: None,
                rpc_username: None,
                rpc_password: None,
            },
            streaming: Default::default(),
        },
        IngestionConfig::default(),
        None,
        metrics.registry(),
    )
    .await?;

    // Register handlers based on selected packages
    for package in &packages {
        match package {
            Package::Deepbook => {
                // DeepBook core event handlers
                indexer
                    .concurrent_pipeline(BalancesHandler::new(env), Default::default())
                    .await?;
                indexer
                    .concurrent_pipeline(DeepBurnedHandler::new(env), Default::default())
                    .await?;
                indexer
                    .concurrent_pipeline(FlashLoanHandler::new(env), Default::default())
                    .await?;
                indexer
                    .concurrent_pipeline(OrderFillHandler::new(env), Default::default())
                    .await?;
                indexer
                    .concurrent_pipeline(OrderUpdateHandler::new(env), Default::default())
                    .await?;
                indexer
                    .concurrent_pipeline(PoolPriceHandler::new(env), Default::default())
                    .await?;
                indexer
                    .concurrent_pipeline(ProposalsHandler::new(env), Default::default())
                    .await?;
                indexer
                    .concurrent_pipeline(RebatesHandler::new(env), Default::default())
                    .await?;
                indexer
                    .concurrent_pipeline(ReferralFeeEventHandler::new(env), Default::default())
                    .await?;
                indexer
                    .concurrent_pipeline(StakesHandler::new(env), Default::default())
                    .await?;
                indexer
                    .concurrent_pipeline(TradeParamsUpdateHandler::new(env), Default::default())
                    .await?;
                indexer
                    .concurrent_pipeline(VotesHandler::new(env), Default::default())
                    .await?;
            }
            Package::DeepbookMargin => {
                indexer
                    .concurrent_pipeline(MarginManagerCreatedHandler::new(env), Default::default())
                    .await?;
                indexer
                    .concurrent_pipeline(LoanBorrowedHandler::new(env), Default::default())
                    .await?;
                indexer
                    .concurrent_pipeline(LoanRepaidHandler::new(env), Default::default())
                    .await?;
                indexer
                    .concurrent_pipeline(LiquidationHandler::new(env), Default::default())
                    .await?;
                indexer
                    .concurrent_pipeline(AssetSuppliedHandler::new(env), Default::default())
                    .await?;
                indexer
                    .concurrent_pipeline(AssetWithdrawnHandler::new(env), Default::default())
                    .await?;
                indexer
                    .concurrent_pipeline(MarginPoolCreatedHandler::new(env), Default::default())
                    .await?;
                indexer
                    .concurrent_pipeline(DeepbookPoolUpdatedHandler::new(env), Default::default())
                    .await?;
                indexer
                    .concurrent_pipeline(InterestParamsUpdatedHandler::new(env), Default::default())
                    .await?;
                indexer
                    .concurrent_pipeline(
                        MarginPoolConfigUpdatedHandler::new(env),
                        Default::default(),
                    )
                    .await?;
                indexer
                    .concurrent_pipeline(MaintainerCapUpdatedHandler::new(env), Default::default())
                    .await?;
                indexer
                    .concurrent_pipeline(
                        DeepbookPoolRegisteredHandler::new(env),
                        Default::default(),
                    )
                    .await?;
                indexer
                    .concurrent_pipeline(
                        DeepbookPoolUpdatedRegistryHandler::new(env),
                        Default::default(),
                    )
                    .await?;
                indexer
                    .concurrent_pipeline(
                        DeepbookPoolConfigUpdatedHandler::new(env),
                        Default::default(),
                    )
                    .await?;
                indexer
                    .concurrent_pipeline(
                        MaintainerFeesWithdrawnHandler::new(env),
                        Default::default(),
                    )
                    .await?;
                indexer
                    .concurrent_pipeline(ProtocolFeesWithdrawnHandler::new(env), Default::default())
                    .await?;
                indexer
                    .concurrent_pipeline(SupplierCapMintedHandler::new(env), Default::default())
                    .await?;
                indexer
                    .concurrent_pipeline(SupplyReferralMintedHandler::new(env), Default::default())
                    .await?;
                indexer
                    .concurrent_pipeline(PauseCapUpdatedHandler::new(env), Default::default())
                    .await?;
                indexer
                    .concurrent_pipeline(ProtocolFeesIncreasedHandler::new(env), Default::default())
                    .await?;
                indexer
                    .concurrent_pipeline(ReferralFeesClaimedHandler::new(env), Default::default())
                    .await?;

                // Collateral Events
                indexer
                    .concurrent_pipeline(DepositCollateralHandler::new(env), Default::default())
                    .await?;
                indexer
                    .concurrent_pipeline(WithdrawCollateralHandler::new(env), Default::default())
                    .await?;

                // TPSL (Take Profit / Stop Loss) Events
                indexer
                    .concurrent_pipeline(ConditionalOrderAddedHandler::new(env), Default::default())
                    .await?;
                indexer
                    .concurrent_pipeline(
                        ConditionalOrderCancelledHandler::new(env),
                        Default::default(),
                    )
                    .await?;
                indexer
                    .concurrent_pipeline(
                        ConditionalOrderExecutedHandler::new(env),
                        Default::default(),
                    )
                    .await?;
                indexer
                    .concurrent_pipeline(
                        ConditionalOrderInsufficientFundsHandler::new(env),
                        Default::default(),
                    )
                    .await?;
            }
        }
    }

    let s_indexer = indexer.run().await?;
    let s_metrics = metrics.run().await?;

    s_indexer.attach(s_metrics).main().await?;
    Ok(())
}

async fn run_walrus_verification(
    config: CheckpointStorageConfig,
    start: Option<u64>,
    limit: Option<u64>,
    from_latest: bool,
) -> Result<(), anyhow::Error> {
    tracing::info!("Initializing checkpoint storage: {}", config.storage);

    match config.storage {
        CheckpointStorageType::Sui => {
            let storage = SuiCheckpointStorage::new(
                Url::parse("https://checkpoints.mainnet.sui.io").unwrap(),
            );

            let count = limit.unwrap_or(1000);
            let mut start_cp = start.unwrap_or(0);

            if from_latest {
                if let Some(latest) = storage.get_latest_checkpoint().await? {
                    start_cp = latest.saturating_sub(count).saturating_add(1);
                }
            }

            tracing::info!(
                "Fetching {} checkpoints starting from {}...",
                count,
                start_cp
            );

            let start_time = std::time::Instant::now();
            let checkpoints = storage.get_checkpoints(start_cp..start_cp + count).await?;

            if checkpoints.is_empty() {
                return Err(anyhow::anyhow!("no checkpoints fetched"));
            }

            for data in checkpoints.iter() {
                tracing::info!(
                    "✓ Fetched checkpoint {}: txs={}, time={}",
                    data.checkpoint_summary.sequence_number,
                    data.transactions.len(),
                    data.checkpoint_summary.timestamp_ms
                );
            }

            let elapsed = start_time.elapsed();
            tracing::info!(
                "Verification complete! Fetched {} checkpoints in {:.2}s ({:.2} cp/s)",
                count,
                elapsed.as_secs_f64(),
                count as f64 / elapsed.as_secs_f64()
            );

            Ok(())
        }
        CheckpointStorageType::Walrus => {
            let storage = WalrusCheckpointStorage::new(
                config.walrus_archival_url.clone(),
                config.walrus_aggregator_url.clone(),
                config.cache_dir.clone(),
                config.cache_enabled,
                config.cache_max_size_gb,
                config.walrus_cli_path.clone(),
                config.walrus_cli_context.clone(),
                config.walrus_cli_skip_consistency_check,
                config.walrus_cli_timeout_secs,
                config.walrus_cli_size_only_fallback,
                config.walrus_coalesce_gap_bytes,
                config.walrus_coalesce_max_range_bytes,
                config.walrus_coalesce_max_range_bytes_cli,
                config.walrus_cli_blob_concurrency,
                config.walrus_cli_range_concurrency,
                config.walrus_cli_range_max_retries,
                config.walrus_cli_range_small_max_retries,
                config.walrus_cli_range_retry_delay_secs,
                config.walrus_cli_range_min_split_bytes,
                config.walrus_http_timeout_secs,
            )?;

            storage.initialize().await?;

            let mut count = limit.unwrap_or(1000);
            let mut start_cp = start.unwrap_or(0);

            if let Some((min_start, max_end)) = storage.coverage_range().await {
                if from_latest {
                    start_cp = max_end.saturating_sub(count).saturating_add(1);
                } else if start.is_none() {
                    start_cp = min_start;
                }

                if start_cp < min_start {
                    tracing::warn!(
                        "start checkpoint {} is before coverage min {}; adjusting",
                        start_cp,
                        min_start
                    );
                    start_cp = min_start;
                }
                if start_cp > max_end {
                    return Err(anyhow::anyhow!(
                        "start checkpoint {} is beyond coverage max {}",
                        start_cp,
                        max_end
                    ));
                }
                let max_count = max_end.saturating_sub(start_cp).saturating_add(1);
                if count > max_count {
                    tracing::warn!(
                        "requested {} checkpoints but only {} available in range; adjusting",
                        count,
                        max_count
                    );
                    count = max_count;
                }
            }

            tracing::info!(
                "Fetching {} checkpoints starting from {}...",
                count,
                start_cp
            );

            let start_time = std::time::Instant::now();
            let checkpoints = storage.get_checkpoints(start_cp..start_cp + count).await?;

            if checkpoints.is_empty() {
                return Err(anyhow::anyhow!(
                    "no checkpoints fetched; range may be outside coverage"
                ));
            }

            for data in checkpoints.iter() {
                tracing::info!(
                    "✓ Fetched checkpoint {}: txs={}, time={}",
                    data.checkpoint_summary.sequence_number,
                    data.transactions.len(),
                    data.checkpoint_summary.timestamp_ms
                );
            }

            let elapsed = start_time.elapsed();
            tracing::info!(
                "Verification complete! Fetched {} checkpoints in {:.2}s ({:.2} cp/s)",
                count,
                elapsed.as_secs_f64(),
                count as f64 / elapsed.as_secs_f64()
            );

            Ok(())
        }
    }
}

async fn run_walrus_download(
    config: CheckpointStorageConfig,
    start: Option<u64>,
    limit: Option<u64>,
    output_dir: PathBuf,
) -> Result<(), anyhow::Error> {
    tracing::info!("Initializing Walrus storage for download...");

    let cli_context = config.walrus_cli_context.clone();
    let storage = WalrusCheckpointStorage::new(
        config.walrus_archival_url,
        config.walrus_aggregator_url,
        config.cache_dir,
        config.cache_enabled,
        config.cache_max_size_gb,
        config.walrus_cli_path,
        cli_context.clone(),
        config.walrus_cli_skip_consistency_check,
        config.walrus_cli_timeout_secs,
        config.walrus_cli_size_only_fallback,
        config.walrus_coalesce_gap_bytes,
        config.walrus_coalesce_max_range_bytes,
        config.walrus_coalesce_max_range_bytes_cli,
        config.walrus_cli_blob_concurrency,
        config.walrus_cli_range_concurrency,
        config.walrus_cli_range_max_retries,
        config.walrus_cli_range_small_max_retries,
        config.walrus_cli_range_retry_delay_secs,
        config.walrus_cli_range_min_split_bytes,
        config.walrus_http_timeout_secs,
    )?;

    // Initialize blob metadata
    storage.initialize().await?;

    let mut count = limit.unwrap_or(10000); // Default to a larger batch for download
    let mut start_cp = start.unwrap_or(0);

    if let Some((min_start, max_end)) = storage.coverage_range().await {
        if start.is_none() {
            start_cp = min_start;
        }

        if start_cp < min_start {
            tracing::warn!(
                "start checkpoint {} is before coverage min {}; adjusting",
                start_cp,
                min_start
            );
            start_cp = min_start;
        }
        if start_cp > max_end {
            return Err(anyhow::anyhow!(
                "start checkpoint {} is beyond coverage max {}",
                start_cp,
                max_end
            ));
        }
        let max_count = max_end.saturating_sub(start_cp).saturating_add(1);
        if count > max_count {
            tracing::warn!(
                "requested {} checkpoints but only {} available in range; adjusting",
                count,
                max_count
            );
            count = max_count;
        }
    }

    let range = start_cp..start_cp + count;

    tracing::info!(
        "Downloading {} checkpoints ({}-{}) to {}...",
        count,
        range.start,
        range.end,
        output_dir.display()
    );

    let start_time = std::time::Instant::now();

    storage.download_checkpoints_to_dir(range, output_dir).await?;

    let elapsed = start_time.elapsed();
    tracing::info!(
        "Download complete! Saved {} checkpoints in {:.2}s ({:.2} cp/s)",
        count,
        elapsed.as_secs_f64(),
        count as f64 / elapsed.as_secs_f64()
    );

    Ok(())
}

async fn run_walrus_package_index(
    config: CheckpointStorageConfig,
    package_id: Option<String>,
    start: Option<u64>,
    limit: Option<u64>,
    from_latest: bool,
    output_path: Option<PathBuf>,
) -> Result<(), anyhow::Error> {
    use std::str::FromStr;

    let package_id = package_id
        .ok_or_else(|| anyhow::anyhow!("--walrus-package-id is required"))?;
    let package_id = ObjectID::from_str(&package_id)
        .with_context(|| format!("invalid package id: {}", package_id))?;

    let cli_context = config.walrus_cli_context.clone();
    let storage = WalrusCheckpointStorage::new(
        config.walrus_archival_url,
        config.walrus_aggregator_url,
        config.cache_dir,
        config.cache_enabled,
        config.cache_max_size_gb,
        config.walrus_cli_path,
        cli_context.clone(),
        config.walrus_cli_skip_consistency_check,
        config.walrus_cli_timeout_secs,
        config.walrus_cli_size_only_fallback,
        config.walrus_coalesce_gap_bytes,
        config.walrus_coalesce_max_range_bytes,
        config.walrus_coalesce_max_range_bytes_cli,
        config.walrus_cli_blob_concurrency,
        config.walrus_cli_range_concurrency,
        config.walrus_cli_range_max_retries,
        config.walrus_cli_range_small_max_retries,
        config.walrus_cli_range_retry_delay_secs,
        config.walrus_cli_range_min_split_bytes,
        config.walrus_http_timeout_secs,
    )?;

    storage.initialize().await?;

    let mut count = limit.unwrap_or(10000);
    let mut start_cp = start.unwrap_or(0);

    if let Some((min_start, max_end)) = storage.coverage_range().await {
        if from_latest {
            start_cp = max_end.saturating_sub(count).saturating_add(1);
        } else if start.is_none() {
            start_cp = min_start;
        }

        if start_cp < min_start {
            tracing::warn!(
                "start checkpoint {} is before coverage min {}; adjusting",
                start_cp,
                min_start
            );
            start_cp = min_start;
        }
        if start_cp > max_end {
            return Err(anyhow::anyhow!(
                "start checkpoint {} is beyond coverage max {}",
                start_cp,
                max_end
            ));
        }
        let max_count = max_end.saturating_sub(start_cp).saturating_add(1);
        if count > max_count {
            tracing::warn!(
                "requested {} checkpoints but only {} available in range; adjusting",
                count,
                max_count
            );
            count = max_count;
        }
    }

    let range = start_cp..start_cp + count;

    tracing::info!(
        "Scanning package {} over checkpoints {}..{}",
        package_id,
        range.start,
        range.end
    );

    let index = storage
        .build_package_checkpoint_index(range, package_id, output_path.clone())
        .await?;

    tracing::info!(
        "Package index complete: {} checkpoints matched",
        index.hits.len()
    );

    if let Some(path) = output_path {
        tracing::info!("Package index written to {}", path.display());
    }

    Ok(())
}

async fn run_walrus_cli_backfill(
    config: CheckpointStorageConfig,
    env: DeepbookEnv,
    packages: Vec<Package>,
    db_args: DbArgs,
    database_url: Url,
    indexer_args: IndexerArgs,
) -> Result<(), anyhow::Error> {
    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct BackfillState {
        next_checkpoint: u64,
        updated_at: String,
    }

    fn load_backfill_state(path: &PathBuf) -> Option<BackfillState> {
        let data = std::fs::read_to_string(path).ok()?;
        serde_json::from_str(&data).ok()
    }

    fn save_backfill_state(path: &PathBuf, next_checkpoint: u64) -> Result<(), anyhow::Error> {
        let state = BackfillState {
            next_checkpoint,
            updated_at: chrono::Utc::now().to_rfc3339(),
        };
        let payload = serde_json::to_string_pretty(&state)?;
        std::fs::write(path, payload)?;
        Ok(())
    }

    if config.storage != CheckpointStorageType::Walrus {
        return Err(anyhow::anyhow!(
            "--walrus-cli-backfill requires CHECKPOINT_STORAGE=walrus (CLI-only mode)"
        ));
    }

    let mut start = indexer_args
        .first_checkpoint
        .ok_or_else(|| anyhow::anyhow!("--first-checkpoint is required for walrus CLI backfill"))?;
    let mut end = indexer_args
        .last_checkpoint
        .ok_or_else(|| anyhow::anyhow!("--last-checkpoint is required for walrus CLI backfill"))?;

    if end < start {
        return Err(anyhow::anyhow!(
            "invalid range: last-checkpoint {} is before first-checkpoint {}",
            end,
            start
        ));
    }

    if config.walrus_cli_path.is_none() {
        return Err(anyhow::anyhow!(
            "--walrus-cli-path must be set for walrus CLI backfill"
        ));
    }

    if config.cache_enabled {
        tracing::warn!(
            "cache enabled; walrus CLI will download full blobs to disk (CHECKPOINT_CACHE_ENABLED=false disables this)"
        );
    }

    let cli_context = config.walrus_cli_context.clone();
    let storage = WalrusCheckpointStorage::new(
        config.walrus_archival_url,
        config.walrus_aggregator_url,
        config.cache_dir,
        config.cache_enabled,
        config.cache_max_size_gb,
        config.walrus_cli_path,
        cli_context.clone(),
        config.walrus_cli_skip_consistency_check,
        config.walrus_cli_timeout_secs,
        config.walrus_cli_size_only_fallback,
        config.walrus_coalesce_gap_bytes,
        config.walrus_coalesce_max_range_bytes,
        config.walrus_coalesce_max_range_bytes_cli,
        config.walrus_cli_blob_concurrency,
        config.walrus_cli_range_concurrency,
        config.walrus_cli_range_max_retries,
        config.walrus_cli_range_small_max_retries,
        config.walrus_cli_range_retry_delay_secs,
        config.walrus_cli_range_min_split_bytes,
        config.walrus_http_timeout_secs,
    )?;

    storage.initialize().await?;

    if let Some((min_start, max_end)) = storage.coverage_range().await {
        if start < min_start {
            tracing::warn!(
                "start checkpoint {} is before coverage min {}; adjusting",
                start,
                min_start
            );
            start = min_start;
        }
        if end > max_end {
            tracing::warn!(
                "end checkpoint {} is beyond coverage max {}; adjusting",
                end,
                max_end
            );
            end = max_end;
        }
        if end < start {
            return Err(anyhow::anyhow!(
                "adjusted range is empty: {}..{}",
                start,
                end
            ));
        }
    }

    let store = Db::for_write(database_url, db_args)
        .await
        .context("Failed to connect to database")?;
    store
        .run_migrations(Some(&MIGRATIONS))
        .await
        .context("Failed to run pending migrations")?;

    let conn = store.connect().await?;
    let conn = std::sync::Arc::new(tokio::sync::Mutex::new(conn));

    let enable_core = packages.iter().any(|p| matches!(p, Package::Deepbook));
    let enable_margin = packages
        .iter()
        .any(|p| matches!(p, Package::DeepbookMargin));

    // Core handlers
    let balances = BalancesHandler::new(env);
    let deep_burned = DeepBurnedHandler::new(env);
    let flash_loan = FlashLoanHandler::new(env);
    let order_fill = OrderFillHandler::new(env);
    let order_update = OrderUpdateHandler::new(env);
    let pool_price = PoolPriceHandler::new(env);
    let proposals = ProposalsHandler::new(env);
    let rebates = RebatesHandler::new(env);
    let referral_fee_events = ReferralFeeEventHandler::new(env);
    let stakes = StakesHandler::new(env);
    let trade_params_update = TradeParamsUpdateHandler::new(env);
    let votes = VotesHandler::new(env);

    // Margin handlers
    let margin_manager_created = MarginManagerCreatedHandler::new(env);
    let loan_borrowed = LoanBorrowedHandler::new(env);
    let loan_repaid = LoanRepaidHandler::new(env);
    let liquidation = LiquidationHandler::new(env);
    let asset_supplied = AssetSuppliedHandler::new(env);
    let asset_withdrawn = AssetWithdrawnHandler::new(env);
    let margin_pool_created = MarginPoolCreatedHandler::new(env);
    let deepbook_pool_updated = DeepbookPoolUpdatedHandler::new(env);
    let interest_params_updated = InterestParamsUpdatedHandler::new(env);
    let margin_pool_config_updated = MarginPoolConfigUpdatedHandler::new(env);
    let maintainer_cap_updated = MaintainerCapUpdatedHandler::new(env);
    let deepbook_pool_registered = DeepbookPoolRegisteredHandler::new(env);
    let deepbook_pool_updated_registry = DeepbookPoolUpdatedRegistryHandler::new(env);
    let deepbook_pool_config_updated = DeepbookPoolConfigUpdatedHandler::new(env);
    let maintainer_fees_withdrawn = MaintainerFeesWithdrawnHandler::new(env);
    let protocol_fees_withdrawn = ProtocolFeesWithdrawnHandler::new(env);
    let supplier_cap_minted = SupplierCapMintedHandler::new(env);
    let supply_referral_minted = SupplyReferralMintedHandler::new(env);
    let protocol_fees_increased = ProtocolFeesIncreasedHandler::new(env);
    let referral_fees_claimed = ReferralFeesClaimedHandler::new(env);
    let deposit_collateral = DepositCollateralHandler::new(env);
    let withdraw_collateral = WithdrawCollateralHandler::new(env);
    let conditional_order_added = ConditionalOrderAddedHandler::new(env);
    let conditional_order_cancelled = ConditionalOrderCancelledHandler::new(env);
    let conditional_order_executed = ConditionalOrderExecutedHandler::new(env);
    let conditional_order_insufficient_funds = ConditionalOrderInsufficientFundsHandler::new(env);

    let end_exclusive = end.saturating_add(1);
    let chunk_size: u64 = std::env::var("WALRUS_CLI_BACKFILL_CHUNK_SIZE")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(200);
    tracing::info!("Walrus CLI backfill chunk size: {}", chunk_size);
    tracing::info!(
        "Walrus CLI concurrency: blobs={}, ranges_per_blob={}, max_range_bytes_cli={}",
        config.walrus_cli_blob_concurrency,
        config.walrus_cli_range_concurrency,
        config.walrus_coalesce_max_range_bytes_cli
    );
    tracing::info!(
        "Walrus CLI range retries: max={}, small_max={}, delay={}s, min_split_bytes={}",
        config.walrus_cli_range_max_retries,
        config.walrus_cli_range_small_max_retries,
        config.walrus_cli_range_retry_delay_secs,
        config.walrus_cli_range_min_split_bytes
    );

    let state_path = PathBuf::from(
        std::env::var("WALRUS_CLI_BACKFILL_STATE_PATH")
            .unwrap_or_else(|_| "backfill_progress.state".to_string()),
    );
    let resume_enabled = std::env::var("WALRUS_CLI_BACKFILL_RESUME")
        .map(|v| v != "false" && v != "0")
        .unwrap_or(true);
    if resume_enabled && state_path.exists() {
        if let Some(state) = load_backfill_state(&state_path) {
            if state.next_checkpoint >= start && state.next_checkpoint < end_exclusive {
                tracing::info!(
                    "resuming walrus CLI backfill from {} (state file {})",
                    state.next_checkpoint,
                    state_path.display()
                );
                start = state.next_checkpoint;
            }
        }
    }

    let max_retries: usize = std::env::var("WALRUS_CLI_BACKFILL_RETRY_ATTEMPTS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(5);
    let retry_delay_secs: u64 = std::env::var("WALRUS_CLI_BACKFILL_RETRY_DELAY_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(15);
    let state_every: u64 = std::env::var("WALRUS_CLI_BACKFILL_STATE_EVERY")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(1);

    let mut next = start;
    let mut bytes_prev = storage.bytes_downloaded();

    tracing::info!(
        "Walrus CLI backfill range: {}..{} (context={})",
        start,
        end,
        cli_context
    );

    while next < end_exclusive {
        if let Some(bad_end) = storage.bad_blob_end_for_checkpoint(next).await {
            tracing::warn!(
                "skipping bad blob covering checkpoints {}..{}",
                next,
                bad_end
            );
            next = bad_end.saturating_add(1);
            let _ = save_backfill_state(&state_path, next);
            continue;
        }

        let chunk_end = (next + chunk_size).min(end_exclusive);
        let t0 = Instant::now();
        let mut attempt = 0usize;
        #[derive(Default)]
        struct Progress {
            count: u64,
            last_processed: Option<u64>,
            processed_since_state: u64,
        }
        let progress = std::sync::Arc::new(tokio::sync::Mutex::new(Progress::default()));

        loop {
            attempt += 1;
            let balances = &balances;
            let deep_burned = &deep_burned;
            let flash_loan = &flash_loan;
            let order_fill = &order_fill;
            let order_update = &order_update;
            let pool_price = &pool_price;
            let proposals = &proposals;
            let rebates = &rebates;
            let referral_fee_events = &referral_fee_events;
            let stakes = &stakes;
            let trade_params_update = &trade_params_update;
            let votes = &votes;
            let margin_manager_created = &margin_manager_created;
            let loan_borrowed = &loan_borrowed;
            let loan_repaid = &loan_repaid;
            let liquidation = &liquidation;
            let asset_supplied = &asset_supplied;
            let asset_withdrawn = &asset_withdrawn;
            let margin_pool_created = &margin_pool_created;
            let deepbook_pool_updated = &deepbook_pool_updated;
            let interest_params_updated = &interest_params_updated;
            let margin_pool_config_updated = &margin_pool_config_updated;
            let maintainer_cap_updated = &maintainer_cap_updated;
            let deepbook_pool_registered = &deepbook_pool_registered;
            let deepbook_pool_updated_registry = &deepbook_pool_updated_registry;
            let deepbook_pool_config_updated = &deepbook_pool_config_updated;
            let maintainer_fees_withdrawn = &maintainer_fees_withdrawn;
            let protocol_fees_withdrawn = &protocol_fees_withdrawn;
            let supplier_cap_minted = &supplier_cap_minted;
            let supply_referral_minted = &supply_referral_minted;
            let protocol_fees_increased = &protocol_fees_increased;
            let referral_fees_claimed = &referral_fees_claimed;
            let deposit_collateral = &deposit_collateral;
            let withdraw_collateral = &withdraw_collateral;
            let conditional_order_added = &conditional_order_added;
            let conditional_order_cancelled = &conditional_order_cancelled;
            let conditional_order_executed = &conditional_order_executed;
            let conditional_order_insufficient_funds = &conditional_order_insufficient_funds;

            let conn = conn.clone();
            let progress = progress.clone();
            let state_path = state_path.clone();
            let result = storage
                .stream_checkpoints(next..chunk_end, |checkpoint_data| {
                    let checkpoint: Checkpoint = checkpoint_data.into();
                    let checkpoint = Arc::new(checkpoint);
                    let seq = checkpoint.summary.sequence_number;

                    let conn = conn.clone();
                    let progress = progress.clone();
                    let state_path = state_path.clone();
                    async move {
                        let mut conn = conn.lock().await;
                        if enable_core {
                            let rows = balances.process(&checkpoint).await?;
                            balances.commit(&rows, &mut conn).await?;
                            let rows = deep_burned.process(&checkpoint).await?;
                            deep_burned.commit(&rows, &mut conn).await?;
                            let rows = flash_loan.process(&checkpoint).await?;
                            flash_loan.commit(&rows, &mut conn).await?;
                            let rows = order_fill.process(&checkpoint).await?;
                            order_fill.commit(&rows, &mut conn).await?;
                            let rows = order_update.process(&checkpoint).await?;
                            order_update.commit(&rows, &mut conn).await?;
                            let rows = pool_price.process(&checkpoint).await?;
                            pool_price.commit(&rows, &mut conn).await?;
                            let rows = proposals.process(&checkpoint).await?;
                            proposals.commit(&rows, &mut conn).await?;
                            let rows = rebates.process(&checkpoint).await?;
                            rebates.commit(&rows, &mut conn).await?;
                            let rows = referral_fee_events.process(&checkpoint).await?;
                            referral_fee_events.commit(&rows, &mut conn).await?;
                            let rows = stakes.process(&checkpoint).await?;
                            stakes.commit(&rows, &mut conn).await?;
                            let rows = trade_params_update.process(&checkpoint).await?;
                            trade_params_update.commit(&rows, &mut conn).await?;
                            let rows = votes.process(&checkpoint).await?;
                            votes.commit(&rows, &mut conn).await?;
                        }

                        if enable_margin {
                            let rows = margin_manager_created.process(&checkpoint).await?;
                            margin_manager_created.commit(&rows, &mut conn).await?;
                            let rows = loan_borrowed.process(&checkpoint).await?;
                            loan_borrowed.commit(&rows, &mut conn).await?;
                            let rows = loan_repaid.process(&checkpoint).await?;
                            loan_repaid.commit(&rows, &mut conn).await?;
                            let rows = liquidation.process(&checkpoint).await?;
                            liquidation.commit(&rows, &mut conn).await?;
                            let rows = asset_supplied.process(&checkpoint).await?;
                            asset_supplied.commit(&rows, &mut conn).await?;
                            let rows = asset_withdrawn.process(&checkpoint).await?;
                            asset_withdrawn.commit(&rows, &mut conn).await?;
                            let rows = margin_pool_created.process(&checkpoint).await?;
                            margin_pool_created.commit(&rows, &mut conn).await?;
                            let rows = deepbook_pool_updated.process(&checkpoint).await?;
                            deepbook_pool_updated.commit(&rows, &mut conn).await?;
                            let rows = interest_params_updated.process(&checkpoint).await?;
                            interest_params_updated.commit(&rows, &mut conn).await?;
                            let rows = margin_pool_config_updated.process(&checkpoint).await?;
                            margin_pool_config_updated.commit(&rows, &mut conn).await?;
                            let rows = maintainer_cap_updated.process(&checkpoint).await?;
                            maintainer_cap_updated.commit(&rows, &mut conn).await?;
                            let rows = deepbook_pool_registered.process(&checkpoint).await?;
                            deepbook_pool_registered.commit(&rows, &mut conn).await?;
                            let rows = deepbook_pool_updated_registry.process(&checkpoint).await?;
                            deepbook_pool_updated_registry.commit(&rows, &mut conn).await?;
                            let rows = deepbook_pool_config_updated.process(&checkpoint).await?;
                            deepbook_pool_config_updated.commit(&rows, &mut conn).await?;
                            let rows = maintainer_fees_withdrawn.process(&checkpoint).await?;
                            maintainer_fees_withdrawn.commit(&rows, &mut conn).await?;
                            let rows = protocol_fees_withdrawn.process(&checkpoint).await?;
                            protocol_fees_withdrawn.commit(&rows, &mut conn).await?;
                            let rows = supplier_cap_minted.process(&checkpoint).await?;
                            supplier_cap_minted.commit(&rows, &mut conn).await?;
                            let rows = supply_referral_minted.process(&checkpoint).await?;
                            supply_referral_minted.commit(&rows, &mut conn).await?;
                            let rows = protocol_fees_increased.process(&checkpoint).await?;
                            protocol_fees_increased.commit(&rows, &mut conn).await?;
                            let rows = referral_fees_claimed.process(&checkpoint).await?;
                            referral_fees_claimed.commit(&rows, &mut conn).await?;
                            let rows = deposit_collateral.process(&checkpoint).await?;
                            deposit_collateral.commit(&rows, &mut conn).await?;
                            let rows = withdraw_collateral.process(&checkpoint).await?;
                            withdraw_collateral.commit(&rows, &mut conn).await?;
                            let rows = conditional_order_added.process(&checkpoint).await?;
                            conditional_order_added.commit(&rows, &mut conn).await?;
                            let rows = conditional_order_cancelled.process(&checkpoint).await?;
                            conditional_order_cancelled.commit(&rows, &mut conn).await?;
                            let rows = conditional_order_executed.process(&checkpoint).await?;
                            conditional_order_executed.commit(&rows, &mut conn).await?;
                            let rows = conditional_order_insufficient_funds.process(&checkpoint).await?;
                            conditional_order_insufficient_funds.commit(&rows, &mut conn).await?;
                        }

                        let mut progress = progress.lock().await;
                        progress.last_processed = Some(seq);
                        progress.count += 1;
                        progress.processed_since_state += 1;
                        if progress.processed_since_state >= state_every {
                            let _ = save_backfill_state(&state_path, seq.saturating_add(1));
                            progress.processed_since_state = 0;
                        }
                        Ok(())
                    }
                })
                .await;

            match result {
                Ok(_) => break,
                Err(err) => {
                    tracing::warn!(
                        "walrus CLI backfill chunk {}..{} failed (attempt {}/{}): {}",
                        next,
                        chunk_end.saturating_sub(1),
                        attempt,
                        max_retries,
                        err
                    );
                    let _ = save_backfill_state(&state_path, next);
                    if attempt >= max_retries {
                        return Err(err);
                    }
                    if let Some(state) = load_backfill_state(&state_path) {
                        if state.next_checkpoint > next && state.next_checkpoint < chunk_end {
                            next = state.next_checkpoint;
                        }
                        if next >= chunk_end {
                            break;
                        }
                    }
                    tokio::time::sleep(std::time::Duration::from_secs(retry_delay_secs)).await;
                }
            }
        }

        let progress_snapshot = progress.lock().await;
        let count = progress_snapshot.count;
        let last_processed = progress_snapshot.last_processed;

        let elapsed = t0.elapsed();
        let seconds = elapsed.as_secs_f64().max(0.0001);
        let cps = count as f64 / seconds;
        let bytes_now = storage.bytes_downloaded();
        let delta_bytes = bytes_now.saturating_sub(bytes_prev);
        let mbps = delta_bytes as f64 / seconds / (1024.0 * 1024.0);

        tracing::info!(
            "processed {} checkpoints ({}..{}) in {:.2}s ({:.2} cp/s, {:.2} MB/s)",
            count,
            next,
            chunk_end.saturating_sub(1),
            elapsed.as_secs_f64(),
            cps,
            mbps
        );

        bytes_prev = bytes_now;
        let next_state = last_processed
            .map(|v| v.saturating_add(1))
            .unwrap_or(chunk_end);
        next = next_state;
        let _ = save_backfill_state(&state_path, next_state);
    }

    Ok(())
}
