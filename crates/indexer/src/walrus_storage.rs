// Copyright (c) DeepBook V3. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use anyhow::{Context, Result};
use async_trait::async_trait;
use byteorder::{LittleEndian, ReadBytesExt};
use futures::stream::{self, StreamExt};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::io::{Cursor, Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use std::time::Instant;
use sui_types::base_types::ObjectID;
use sui_types::full_checkpoint_content::CheckpointData;
use sui_types::messages_checkpoint::CheckpointSequenceNumber;
use tokio::process::Command;
use tokio::sync::RwLock;

use super::checkpoint_storage::CheckpointStorage;

/// Walrus blob metadata
#[derive(Debug, Clone, Deserialize)]
pub struct BlobMetadata {
    #[serde(rename = "blob_id")]
    pub blob_id: String,

    #[serde(rename = "start_checkpoint")]
    pub start_checkpoint: u64,

    #[serde(rename = "end_checkpoint")]
    pub end_checkpoint: u64,

    #[serde(rename = "entries_count")]
    pub entries_count: u64,

    pub total_size: u64,

    #[serde(default)]
    pub end_of_epoch: bool,

    #[serde(default)]
    pub expiry_epoch: u64,
}

/// Walrus blobs list response
#[derive(Debug, Deserialize)]
pub struct BlobsResponse {
    pub blobs: Vec<BlobMetadata>,
}

/// Walrus checkpoint response
#[derive(Debug, Deserialize)]
pub struct WalrusCheckpointResponse {
    pub checkpoint_number: u64,
    pub blob_id: String,
    pub object_id: String,
    pub index: u64,
    pub offset: u64,
    pub length: u64,
    #[serde(default)]
    pub content: Option<serde_json::Value>,
}

/// Parsed index entry from a blob
#[derive(Debug, Clone)]
struct BlobIndexEntry {
    #[allow(dead_code)]
    pub checkpoint_number: u64,
    pub offset: u64,
    pub length: u64,
}

#[derive(Debug, Clone)]
struct CoalescedRange<T> {
    pub start: u64,
    pub end: u64,
    pub entries: Vec<(CheckpointSequenceNumber, BlobIndexEntry, T)>,
}

impl<T> CoalescedRange<T> {
    fn len(&self) -> u64 {
        self.end.saturating_sub(self.start)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PackageCheckpointHit {
    pub checkpoint: CheckpointSequenceNumber,
    pub event_count: u64,
    pub object_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PackageCheckpointIndex {
    pub package_id: ObjectID,
    pub hits: Vec<PackageCheckpointHit>,
}

fn coalesce_entries<T: Clone>(
    entries: &[(CheckpointSequenceNumber, BlobIndexEntry, T)],
    max_gap_bytes: u64,
    max_range_bytes: u64,
) -> Vec<CoalescedRange<T>> {
    if entries.is_empty() {
        return Vec::new();
    }

    let mut sorted = entries.to_vec();
    sorted.sort_by_key(|(_, entry, _)| entry.offset);

    let mut ranges = Vec::new();
    let mut current: Option<CoalescedRange<T>> = None;

    for (checkpoint, entry, payload) in sorted {
        let entry_start = entry.offset;
        let entry_end = entry.offset.saturating_add(entry.length);

        match current.take() {
            Some(mut range) => {
                let gap_ok = entry_start <= range.end.saturating_add(max_gap_bytes);
                let new_end = range.end.max(entry_end);
                let size_ok = new_end.saturating_sub(range.start) <= max_range_bytes;

                if gap_ok && size_ok {
                    range.end = new_end;
                    range.entries.push((checkpoint, entry, payload));
                    current = Some(range);
                } else {
                    ranges.push(range);
                    current = Some(CoalescedRange {
                        start: entry_start,
                        end: entry_end,
                        entries: vec![(checkpoint, entry, payload)],
                    });
                }
            }
            None => {
                current = Some(CoalescedRange {
                    start: entry_start,
                    end: entry_end,
                    entries: vec![(checkpoint, entry, payload)],
                });
            }
        }
    }

    if let Some(range) = current {
        ranges.push(range);
    }

    ranges
}

fn count_package_hits(checkpoint: &CheckpointData, package_id: ObjectID) -> (u64, u64) {
    let mut event_count = 0u64;
    let mut object_count = 0u64;

    for tx in &checkpoint.transactions {
        if let Some(events) = &tx.events {
            event_count += events
                .data
                .iter()
                .filter(|event| event.package_id == package_id)
                .count() as u64;
        }

        for object in tx
            .input_objects
            .iter()
            .chain(tx.output_objects.iter())
        {
            if object.is_package() {
                if object.id() == package_id {
                    object_count += 1;
                }
                continue;
            }

            if let Some(obj_type) = object.type_() {
                if ObjectID::from(obj_type.address()) == package_id {
                    object_count += 1;
                }
            }
        }
    }

    (event_count, object_count)
}

/// Walrus checkpoint storage (blob-based)
///
/// Downloads checkpoints from Walrus aggregator using blob-based storage:
/// 1. Fetch blob metadata from walrus-sui-archival service
/// 2. Download blobs (2-3 GB each) or use local cache
/// 3. Extract checkpoints from blobs using internal index
#[derive(Clone)]
pub struct WalrusCheckpointStorage {
    inner: Arc<Inner>,
}

struct Inner {
    archival_url: String,
    aggregator_url: String,
    cache_dir: PathBuf,
    cache_enabled: bool,
    walrus_cli_path: Option<PathBuf>,
    walrus_cli_context: String,
    walrus_cli_skip_consistency_check: bool,
    walrus_cli_timeout_secs: u64,
    walrus_cli_size_only_fallback: bool,
    coalesce_gap_bytes: u64,
    coalesce_max_range_bytes: u64,
    coalesce_max_range_bytes_cli: u64,
    walrus_cli_blob_concurrency: usize,
    walrus_cli_range_concurrency: usize,
    walrus_cli_range_max_retries: usize,
    walrus_cli_range_small_max_retries: usize,
    walrus_cli_range_retry_delay_secs: u64,
    walrus_cli_range_min_split_bytes: u64,
    client: Client,
    index_cache: RwLock<HashMap<String, HashMap<u64, BlobIndexEntry>>>,
    blob_size_cache: RwLock<HashMap<String, u64>>,
    bad_blobs: RwLock<HashSet<String>>,
    metadata: RwLock<Vec<BlobMetadata>>,
    bytes_downloaded: AtomicU64,
}

impl WalrusCheckpointStorage {
    /// Create a new Walrus checkpoint storage instance
    pub fn new(
        archival_url: String,
        aggregator_url: String,
        cache_dir: PathBuf,
        cache_enabled: bool,
        _cache_max_size: u64,
        walrus_cli_path: Option<PathBuf>,
        walrus_cli_context: String,
        walrus_cli_skip_consistency_check: bool,
        walrus_cli_timeout_secs: u64,
        walrus_cli_size_only_fallback: bool,
        coalesce_gap_bytes: u64,
        coalesce_max_range_bytes: u64,
        coalesce_max_range_bytes_cli: u64,
        walrus_cli_blob_concurrency: usize,
        walrus_cli_range_concurrency: usize,
        walrus_cli_range_max_retries: usize,
        walrus_cli_range_small_max_retries: usize,
        walrus_cli_range_retry_delay_secs: u64,
        walrus_cli_range_min_split_bytes: u64,
        http_timeout_secs: u64,
    ) -> Result<Self> {
        if walrus_cli_path.is_some() && cache_enabled {
            // Ensure cache directory exists if using CLI mode
            std::fs::create_dir_all(&cache_dir).context("failed to create cache dir for walrus cli")?;
        }

        Ok(Self {
            inner: Arc::new(Inner {
                archival_url,
                aggregator_url,
                cache_dir,
                cache_enabled,
                walrus_cli_path,
                walrus_cli_context,
                walrus_cli_skip_consistency_check,
                walrus_cli_timeout_secs,
                walrus_cli_size_only_fallback,
                coalesce_gap_bytes,
                coalesce_max_range_bytes,
                coalesce_max_range_bytes_cli,
                walrus_cli_blob_concurrency: walrus_cli_blob_concurrency.max(1),
                walrus_cli_range_concurrency: walrus_cli_range_concurrency.max(1),
                walrus_cli_range_max_retries: walrus_cli_range_max_retries.max(1),
                walrus_cli_range_small_max_retries: walrus_cli_range_small_max_retries.max(1),
                walrus_cli_range_retry_delay_secs,
                walrus_cli_range_min_split_bytes,
                client: Client::builder()
                    .timeout(std::time::Duration::from_secs(http_timeout_secs))
                    .build()
                    .expect("Failed to create HTTP client"),
                index_cache: RwLock::new(HashMap::new()),
                blob_size_cache: RwLock::new(HashMap::new()),
                bad_blobs: RwLock::new(HashSet::new()),
                metadata: RwLock::new(Vec::new()),
                bytes_downloaded: AtomicU64::new(0),
            }),
        })
    }

    pub fn bytes_downloaded(&self) -> u64 {
        self.inner.bytes_downloaded.load(Ordering::Relaxed)
    }

    pub async fn stream_checkpoints<F, Fut>(
        &self,
        range: std::ops::Range<CheckpointSequenceNumber>,
        mut on_checkpoint: F,
    ) -> Result<u64>
    where
        F: FnMut(CheckpointData) -> Fut,
        Fut: std::future::Future<Output = Result<()>>,
    {
        let metadata = self.inner.metadata.read().await;
        let mut blobs: Vec<BlobMetadata> = metadata
            .iter()
            .filter(|b| b.end_checkpoint >= range.start && b.start_checkpoint < range.end)
            .cloned()
            .collect();
        drop(metadata);

        if blobs.is_empty() {
            return Ok(0);
        }

        blobs.sort_by_key(|b| b.start_checkpoint);
        let mut total_processed = 0u64;

        for blob in blobs {
            if self.inner.walrus_cli_path.is_some() && self.inner.cache_enabled {
                self.download_blob_via_cli(&blob.blob_id).await?;
            }

            let index = match self.load_blob_index(&blob.blob_id).await {
                Ok(index) => index,
                Err(e) if self.inner.walrus_cli_path.is_some() => {
                    self.mark_bad_blob(&blob.blob_id).await;
                    tracing::warn!(
                        "skipping blob {} due to index load error: {}",
                        blob.blob_id,
                        e
                    );
                    continue;
                }
                Err(e) => return Err(e),
            };

            let start_cp = range.start.max(blob.start_checkpoint);
            let end_cp_exclusive = range.end.min(blob.end_checkpoint.saturating_add(1));
            if start_cp >= end_cp_exclusive {
                continue;
            }

            let mut tasks = Vec::new();
            for cp_num in start_cp..end_cp_exclusive {
                if let Some(entry) = index.get(&cp_num) {
                    tasks.push((cp_num, entry.clone()));
                }
            }

            if tasks.is_empty() {
                continue;
            }

            let max_gap_bytes = self.inner.coalesce_gap_bytes;
            let max_range_bytes = if self.inner.walrus_cli_path.is_some() {
                self.inner.coalesce_max_range_bytes_cli
            } else {
                self.inner.coalesce_max_range_bytes
            };

            let pending: Vec<(CheckpointSequenceNumber, BlobIndexEntry, ())> = tasks
                .into_iter()
                .map(|(cp_num, entry)| (cp_num, entry, ()))
                .collect();
            let coalesced = coalesce_entries(&pending, max_gap_bytes, max_range_bytes);

            for range in coalesced {
                if range.len() == 0 {
                    continue;
                }

                let bytes = self
                    .download_range(&blob.blob_id, range.start, range.len())
                    .await?;

                let mut entries = range.entries;
                entries.sort_by_key(|(cp_num, _, _)| *cp_num);

                for (cp_num, entry, _) in entries {
                    let start = entry.offset.saturating_sub(range.start) as usize;
                    let end = start + entry.length as usize;
                    let checkpoint = sui_storage::blob::Blob::from_bytes::<CheckpointData>(
                        &bytes[start..end],
                    )
                    .with_context(|| format!("failed to deserialize checkpoint {}", cp_num))?;
                    on_checkpoint(checkpoint).await?;
                    total_processed += 1;
                }
            }
        }

        Ok(total_processed)
    }

    fn parse_blob_size_from_error(err: &anyhow::Error) -> Option<u64> {
        let msg = err.to_string();
        let marker = "blob size is ";
        let idx = msg.find(marker)?;
        let rest = &msg[idx + marker.len()..];
        let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
        digits.parse::<u64>().ok()
    }

    async fn mark_bad_blob(&self, blob_id: &str) {
        let mut bad = self.inner.bad_blobs.write().await;
        bad.insert(blob_id.to_string());
    }

    pub async fn bad_blob_end_for_checkpoint(&self, checkpoint: u64) -> Option<u64> {
        let blob = self.find_blob_for_checkpoint(checkpoint).await?;
        let bad = self.inner.bad_blobs.read().await;
        if bad.contains(&blob.blob_id) {
            Some(blob.end_checkpoint)
        } else {
            None
        }
    }

    async fn run_cli_with_timeout(&self, mut cmd: Command) -> Result<std::process::Output> {
        cmd.kill_on_drop(true);
        cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
        let child = cmd.spawn().context("failed to spawn walrus cli")?;
        let timeout = Duration::from_secs(self.inner.walrus_cli_timeout_secs);
        match tokio::time::timeout(timeout, child.wait_with_output()).await {
            Ok(res) => res.context("failed to wait on walrus cli"),
            Err(_) => Err(anyhow::anyhow!(
                "walrus cli timed out after {}s",
                self.inner.walrus_cli_timeout_secs
            )),
        }
    }

    /// Initialize by fetching blob metadata from archival service
    pub async fn initialize(&self) -> Result<()> {
        tracing::info!("fetching Walrus blob metadata from: {}", self.inner.archival_url);

        let url = format!("{}/v1/app_blobs", self.inner.archival_url);
        let response = self.inner
            .client
            .get(&url)
            .send()
            .await
            .with_context(|| format!("failed to fetch blobs from: {}", url))?;

        if !response.status().is_success() {
            return Err(anyhow::anyhow!(
                "Walrus archival service returned error status {}",
                response.status()
            ));
        }

        let blobs: BlobsResponse = response
            .json()
            .await
            .context("failed to parse blobs response")?;

        let mut metadata = self.inner.metadata.write().await;
        *metadata = blobs.blobs;

        let min_start = metadata
            .iter()
            .map(|b| b.start_checkpoint)
            .min()
            .unwrap_or(0);
        let max_end = metadata
            .iter()
            .map(|b| b.end_checkpoint)
            .max()
            .unwrap_or(0);

        tracing::info!(
            "fetched {} Walrus blobs covering checkpoints {}..{}",
            metadata.len(),
            min_start,
            max_end
        );

        Ok(())
    }

    /// Get min/max checkpoint coverage from blob metadata
    pub async fn coverage_range(&self) -> Option<(u64, u64)> {
        let metadata = self.inner.metadata.read().await;
        let min_start = metadata.iter().map(|b| b.start_checkpoint).min()?;
        let max_end = metadata.iter().map(|b| b.end_checkpoint).max()?;
        Some((min_start, max_end))
    }

    /// Find blob containing a specific checkpoint
    async fn find_blob_for_checkpoint(&self, checkpoint: u64) -> Option<BlobMetadata> {
        let metadata = self.inner.metadata.read().await;
        metadata
            .iter()
            .find(|blob| checkpoint >= blob.start_checkpoint && checkpoint <= blob.end_checkpoint)
            .cloned()
    }

    /// Load (or fetch) the index for a blob
    async fn load_blob_index(&self, blob_id: &str) -> Result<HashMap<u64, BlobIndexEntry>> {
        // 1. Check memory cache
        {
            let cache = self.inner.index_cache.read().await;
            if let Some(index) = cache.get(blob_id) {
                return Ok(index.clone());
            }
        }

        // 2. Try reading from local file (CLI/Cache mode)
        let cached_path = self.get_cached_blob_path(blob_id);
        if cached_path.exists() {
            tracing::info!("loading index from cached blob {}", cached_path.display());
            let mut file = std::fs::File::open(&cached_path)?;
            
            // Read Footer (last 24 bytes)
            let file_len = file.metadata()?.len();
            if file_len < 24 {
                return Err(anyhow::anyhow!("cached blob too small"));
            }
            file.seek(SeekFrom::Start(file_len - 24))?;
            let magic = file.read_u32::<LittleEndian>()?;
            if magic != 0x574c4244 {
                return Err(anyhow::anyhow!("invalid blob footer magic"));
            }
            let _version = file.read_u32::<LittleEndian>()?;
            let index_start_offset = file.read_u64::<LittleEndian>()?;
            let count = file.read_u32::<LittleEndian>()?;

            // Read Index
            file.seek(SeekFrom::Start(index_start_offset))?;
            // Read remaining bytes for index
            let mut index_bytes = Vec::new();
            file.read_to_end(&mut index_bytes)?;
            
            let mut cursor = Cursor::new(&index_bytes);
            let mut index = HashMap::with_capacity(count as usize);

            for _ in 0..count {
                let name_len = cursor.read_u32::<LittleEndian>()?;
                let mut name_bytes = vec![0u8; name_len as usize];
                cursor.read_exact(&mut name_bytes)?;
                let name_str = String::from_utf8(name_bytes)?;
                let checkpoint_number = name_str.parse::<u64>()?;
                let offset = cursor.read_u64::<LittleEndian>()?;
                let length = cursor.read_u64::<LittleEndian>()?;
                let _entry_crc = cursor.read_u32::<LittleEndian>()?;

                index.insert(checkpoint_number, BlobIndexEntry { checkpoint_number, offset, length });
            }

            // Update Cache
            let mut cache = self.inner.index_cache.write().await;
            cache.insert(blob_id.to_string(), index.clone());
            return Ok(index);
        }

        // 3. If CLI is configured, read footer + index via range reads (no aggregator)
        if self.inner.walrus_cli_path.is_some() {
            let blob_meta = {
                let metadata = self.inner.metadata.read().await;
                metadata.iter().find(|b| b.blob_id == blob_id).cloned()
            }
            .ok_or_else(|| anyhow::anyhow!("missing metadata for blob {}", blob_id))?;

            let mut total_size = self.blob_size_via_cli(blob_id).await?;
            if total_size != blob_meta.total_size {
                tracing::info!(
                    "walrus cli size differs from archival metadata for blob {} (cli={}, archival={})",
                    blob_id,
                    total_size,
                    blob_meta.total_size
                );
            }

            if total_size < 24 {
                return Err(anyhow::anyhow!("blob {} is too small", blob_id));
            }

            let mut footer_start = total_size - 24;
            let mut footer_bytes = match self
                .read_range_via_cli_resilient(blob_id, footer_start, 24)
                .await
            {
                Ok(bytes) => bytes,
                Err(e) => {
                    if let Some(actual_size) = Self::parse_blob_size_from_error(&e) {
                        total_size = actual_size;
                        footer_start = total_size - 24;
                        self.read_range_via_cli_resilient(blob_id, footer_start, 24)
                            .await?
                    } else {
                        return Err(e);
                    }
                }
            };
            if footer_bytes.len() != 24 {
                return Err(anyhow::anyhow!(
                    "invalid footer length for blob {}: {}",
                    blob_id,
                    footer_bytes.len()
                ));
            }

            let mut cursor = Cursor::new(&footer_bytes);
            let mut magic = cursor.read_u32::<LittleEndian>()?;
            if magic != 0x574c4244 {
                // Try to locate footer within the index window (metadata total_size is index start)
                match self
                    .find_footer_in_index_window(
                        blob_id,
                        blob_meta.total_size,
                        blob_meta.entries_count,
                        blob_meta.end_checkpoint,
                    )
                    .await
                {
                    Ok((found_size, footer)) => {
                        total_size = found_size;
                        footer_bytes = footer;
                        let mut retry_cursor = Cursor::new(&footer_bytes);
                        magic = retry_cursor.read_u32::<LittleEndian>()?;
                        if magic != 0x574c4244 {
                            return Err(anyhow::anyhow!(
                                "invalid blob footer magic after scan: {:x}",
                                magic
                            ));
                        }
                        cursor = retry_cursor;
                    }
                    Err(scan_err) => {
                        if self.inner.walrus_cli_size_only_fallback {
                            tracing::warn!(
                                "invalid footer magic from metadata size (blob {}); retrying with size-only",
                                blob_id
                            );
                            total_size = self.blob_size_via_cli(blob_id).await?;
                            let footer_start = total_size - 24;
                            footer_bytes = self
                                .read_range_via_cli_resilient(blob_id, footer_start, 24)
                                .await?;
                            let mut retry_cursor = Cursor::new(&footer_bytes);
                            magic = retry_cursor.read_u32::<LittleEndian>()?;
                            if magic != 0x574c4244 {
                                return Err(anyhow::anyhow!("invalid blob footer magic: {:x}", magic));
                            }
                            cursor = retry_cursor;
                        } else {
                            return Err(scan_err);
                        }
                    }
                }
            }

            let _version = cursor.read_u32::<LittleEndian>()?;
            let index_start_offset = cursor.read_u64::<LittleEndian>()?;
            let count = cursor.read_u32::<LittleEndian>()?;

            if index_start_offset >= total_size {
                return Err(anyhow::anyhow!(
                    "invalid index start offset {} for blob size {}",
                    index_start_offset,
                    total_size
                ));
            }

            let index_len = total_size - index_start_offset;
            let index_bytes = self
                .read_range_via_cli_resilient(blob_id, index_start_offset, index_len)
                .await?;

            let mut cursor = Cursor::new(&index_bytes);
            let mut index = HashMap::with_capacity(count as usize);

            for _ in 0..count {
                let name_len = cursor.read_u32::<LittleEndian>()?;
                let mut name_bytes = vec![0u8; name_len as usize];
                cursor.read_exact(&mut name_bytes)?;
                let name_str = String::from_utf8(name_bytes)?;
                let checkpoint_number = name_str.parse::<u64>()?;
                let offset = cursor.read_u64::<LittleEndian>()?;
                let length = cursor.read_u64::<LittleEndian>()?;
                let _entry_crc = cursor.read_u32::<LittleEndian>()?;

                index.insert(
                    checkpoint_number,
                    BlobIndexEntry {
                        checkpoint_number,
                        offset,
                        length,
                    },
                );
            }

            let mut cache = self.inner.index_cache.write().await;
            cache.insert(blob_id.to_string(), index.clone());
            return Ok(index);
        }

        tracing::info!("fetching index for blob {} from aggregator", blob_id);

        // 3. Fetch Footer (last 24 bytes) from Aggregator
        let url = format!("{}/v1/blobs/{}", self.inner.aggregator_url, blob_id);
        let footer_bytes = self.with_retry(|| {
            let client = self.inner.client.clone();
            let url = url.clone();
            async move {
                let response = client.get(&url)
                    .header("Range", "bytes=-24")
                    .send()
                    .await?;

                if !response.status().is_success() {
                     return Err(anyhow::anyhow!("status {}", response.status()));
                }
                Ok(response.bytes().await?)
            }
        }).await.with_context(|| format!("failed to fetch footer for blob {}", blob_id))?;

        if footer_bytes.len() != 24 {
             return Err(anyhow::anyhow!("invalid footer length: {}", footer_bytes.len()));
        }

        // Parse Footer
        let mut cursor = Cursor::new(&footer_bytes);
        let magic = cursor.read_u32::<LittleEndian>()?;
        if magic != 0x574c4244 { // "DBLW"
            return Err(anyhow::anyhow!("invalid blob footer magic: {:x}", magic));
        }

        let _version = cursor.read_u32::<LittleEndian>()?;
        let index_start_offset = cursor.read_u64::<LittleEndian>()?;
        let count = cursor.read_u32::<LittleEndian>()?;

        // 4. Fetch Index (from start offset to end)
        let index_bytes = self.with_retry(|| {
            let client = self.inner.client.clone();
            let url = url.clone();
            async move {
                let response = client.get(&url)
                    .header("Range", format!("bytes={}-", index_start_offset))
                    .send()
                    .await?;

                if !response.status().is_success() {
                     return Err(anyhow::anyhow!("status {}", response.status()));
                }
                Ok(response.bytes().await?)
            }
        }).await.with_context(|| format!("failed to fetch index for blob {}", blob_id))?;
        
        // Parse Index
        let mut cursor = Cursor::new(&index_bytes);
        let mut index = HashMap::with_capacity(count as usize);

        for _ in 0..count {
            let name_len = cursor.read_u32::<LittleEndian>()?;
            let mut name_bytes = vec![0u8; name_len as usize];
            cursor.read_exact(&mut name_bytes)?;

            let name_str = String::from_utf8(name_bytes)
                .context("invalid utf8 in checkpoint name")?;
            let checkpoint_number = name_str
                .parse::<u64>()
                .context("invalid checkpoint number string")?;

            let offset = cursor.read_u64::<LittleEndian>()?;
            let length = cursor.read_u64::<LittleEndian>()?;
            let _entry_crc = cursor.read_u32::<LittleEndian>()?;

            index.insert(
                checkpoint_number,
                BlobIndexEntry {
                    checkpoint_number,
                    offset,
                    length,
                },
            );
        }

        // 5. Update Cache
        {
            let mut cache = self.inner.index_cache.write().await;
            cache.insert(blob_id.to_string(), index.clone());
        }

        Ok(index)
    }

    /// Helper for aggregator retries
    async fn with_retry<F, Fut, T>(&self, mut f: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let mut attempts = 0;
        let max_attempts = 5;
        loop {
            match f().await {
                Ok(res) => return Ok(res),
                Err(e) if attempts < max_attempts => {
                    attempts += 1;
                    tracing::warn!("aggregator request failed (attempt {}): {}. Retrying...", attempts, e);
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Check if blob exists in cache
    fn get_cached_blob_path(&self, blob_id: &str) -> PathBuf {
        self.inner.cache_dir.join(blob_id)
    }

    /// Download full blob using Walrus CLI
    async fn download_blob_via_cli(&self, blob_id: &str) -> Result<PathBuf> {
        let cli_path = self.inner.walrus_cli_path.as_ref()
            .ok_or_else(|| anyhow::anyhow!("Walrus CLI path not configured"))?;
        
        let output_path = self.get_cached_blob_path(blob_id);
        
        // If file exists and size matches metadata, skip
        if output_path.exists() {
            // Optional: verify size matches metadata
            return Ok(output_path);
        }

        tracing::info!("Starting download of blob {} via CLI to {}", blob_id, output_path.display());
        let start_time = std::time::Instant::now();

        let mut cmd = Command::new(cli_path);
        cmd.arg("read")
            .arg(blob_id)
            .arg("--context")
            .arg(&self.inner.walrus_cli_context)
            .arg("--out")
            .arg(&output_path);
        if self.inner.walrus_cli_skip_consistency_check {
            cmd.arg("--skip-consistency-check");
        }

        let output = self.run_cli_with_timeout(cmd).await?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(anyhow::anyhow!(
                "walrus cli failed with status {}: {}",
                output.status,
                stderr.trim()
            ));
        }

        let elapsed = start_time.elapsed();
        let size_bytes = output_path.metadata()?.len() as u64;
        self.inner.bytes_downloaded.fetch_add(size_bytes, Ordering::Relaxed);
        let size_mb = size_bytes as f64 / 1_000_000.0;
        tracing::info!("Finished download of blob {} in {:.2}s ({:.2} MB/s, total size: {:.2} MB)", 
            blob_id, 
            elapsed.as_secs_f64(),
            size_mb / elapsed.as_secs_f64(),
            size_mb
        );

        Ok(output_path)
    }

    /// Read a specific byte range via Walrus CLI (no disk cache)
    async fn read_range_via_cli(&self, blob_id: &str, start: u64, length: u64) -> Result<Vec<u8>> {
        let cli_path = self.inner.walrus_cli_path.as_ref()
            .ok_or_else(|| anyhow::anyhow!("Walrus CLI path not configured"))?;

        let t0 = Instant::now();
        let mut cmd = Command::new(cli_path);
        cmd.arg("read")
            .arg(blob_id)
            .arg("--context")
            .arg(&self.inner.walrus_cli_context)
            .arg("--stream")
            .arg("--start-byte")
            .arg(start.to_string())
            .arg("--byte-length")
            .arg(length.to_string());
        if self.inner.walrus_cli_skip_consistency_check {
            cmd.arg("--skip-consistency-check");
        }

        let output = self.run_cli_with_timeout(cmd).await?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(anyhow::anyhow!("walrus cli failed: {}", stderr.trim()));
        }

        let elapsed = t0.elapsed().as_secs_f64().max(1e-6);
        self.inner.bytes_downloaded.fetch_add(output.stdout.len() as u64, Ordering::Relaxed);
        let len = output.stdout.len() as u64;
        if len >= 4 * 1024 * 1024 {
            let mb = len as f64 / 1_000_000.0;
            tracing::info!(
                "walrus cli read {:.2} MB from {} (offset {}, len {}) in {:.2}s ({:.2} MB/s)",
                mb,
                blob_id,
                start,
                len,
                elapsed,
                mb / elapsed
            );
        }
        Ok(output.stdout)
    }

    async fn read_range_via_cli_resilient(
        &self,
        blob_id: &str,
        start: u64,
        length: u64,
    ) -> Result<Vec<u8>> {
        let max_retries = self.inner.walrus_cli_range_max_retries.max(1);
        let max_retries_small = self.inner.walrus_cli_range_small_max_retries.max(1);
        let retry_delay_secs = self.inner.walrus_cli_range_retry_delay_secs;
        let min_split_bytes = self.inner.walrus_cli_range_min_split_bytes;

        let mut output = vec![0u8; length as usize];
        let mut pending: Vec<(u64, u64)> = vec![(start, length)];

        while let Some((range_start, range_len)) = pending.pop() {
            let mut last_err: Option<anyhow::Error> = None;
            let mut success = None;
            let max_retries = if range_len <= 1024 * 1024 {
                max_retries_small
            } else {
                max_retries
            };

            for attempt in 1..=max_retries {
                match self.read_range_via_cli(blob_id, range_start, range_len).await {
                    Ok(bytes) => {
                        success = Some(bytes);
                        last_err = None;
                        break;
                    }
                    Err(e) => {
                        tracing::warn!(
                        "walrus cli range read failed (attempt {}/{}): {}",
                        attempt,
                        max_retries,
                        e
                    );
                    last_err = Some(e);
                    if attempt < max_retries {
                        let delay = retry_delay_secs.saturating_mul(attempt as u64).max(1);
                        tokio::time::sleep(Duration::from_secs(delay)).await;
                    }
                }
            }
            }

            if let Some(bytes) = success {
                let offset = range_start
                    .checked_sub(start)
                    .ok_or_else(|| anyhow::anyhow!("invalid range start"))? as usize;
                let end = offset + range_len as usize;
                output[offset..end].copy_from_slice(&bytes);
                continue;
            }

            if min_split_bytes == 0 || range_len <= min_split_bytes {
                return Err(last_err.unwrap_or_else(|| anyhow::anyhow!("walrus cli range read failed")));
            }

            let left_len = range_len / 2;
            let right_len = range_len - left_len;
            tracing::warn!(
                "splitting walrus cli range read ({} bytes) into {} + {}",
                range_len,
                left_len,
                right_len
            );
            // Push right then left so left is processed first.
            pending.push((range_start + left_len, right_len));
            pending.push((range_start, left_len));
        }

        Ok(output)
    }

    async fn blob_size_via_cli(&self, blob_id: &str) -> Result<u64> {
        let cli_path = self.inner.walrus_cli_path.as_ref()
            .ok_or_else(|| anyhow::anyhow!("Walrus CLI path not configured"))?;

        // Check cache first
        {
            let cache = self.inner.blob_size_cache.read().await;
            if let Some(size) = cache.get(blob_id) {
                return Ok(*size);
            }
        }

        let mut stdout = String::new();
        let mut last_err: Option<anyhow::Error> = None;
        for attempt in 1..=3 {
            let mut cmd = Command::new(cli_path);
            cmd.arg("read")
                .arg(blob_id)
                .arg("--context")
                .arg(&self.inner.walrus_cli_context)
                .arg("--size-only")
                .arg("--json");
            if self.inner.walrus_cli_skip_consistency_check {
                cmd.arg("--skip-consistency-check");
            }

            let output = self.run_cli_with_timeout(cmd).await?;

            if output.status.success() {
                stdout = String::from_utf8_lossy(&output.stdout).to_string();
                last_err = None;
                break;
            }

            let stderr = String::from_utf8_lossy(&output.stderr);
            last_err = Some(anyhow::anyhow!(
                "walrus cli size-only failed with status {}: {}",
                output.status,
                stderr.trim()
            ));
            tracing::warn!("walrus cli size-only failed (attempt {}/3)", attempt);
            tokio::time::sleep(Duration::from_secs(2)).await;
        }

        if let Some(err) = last_err {
            return Err(err);
        }
        let trimmed = stdout.trim();
        let mut size: Option<u64> = None;
        if let Ok(val) = serde_json::from_str::<serde_json::Value>(trimmed) {
            if let Some(s) = val.get("size").and_then(|v| v.as_u64()) {
                size = Some(s);
            } else if let Some(s) = val.get("blobSize").and_then(|v| v.as_u64()) {
                size = Some(s);
            } else if let Some(s) = val.get("blob_size").and_then(|v| v.as_u64()) {
                size = Some(s);
            } else if let Some(s) = val.get("unencoded_size").and_then(|v| v.as_u64()) {
                size = Some(s);
            } else if let Some(s) = val.as_u64() {
                size = Some(s);
            }
        } else if let Ok(s) = trimmed.parse::<u64>() {
            size = Some(s);
        }

        let size = size.ok_or_else(|| anyhow::anyhow!("unable to parse blob size from walrus cli output: {}", trimmed))?;

        let mut cache = self.inner.blob_size_cache.write().await;
        cache.insert(blob_id.to_string(), size);
        Ok(size)
    }

    async fn find_footer_in_index_window(
        &self,
        blob_id: &str,
        index_start_guess: u64,
        entries_count: u64,
        end_checkpoint: u64,
    ) -> Result<(u64, Vec<u8>)> {
        let name_len = end_checkpoint.to_string().len() as u64;
        let approx_index_len = entries_count.saturating_mul(name_len + 24);
        let window_len = approx_index_len
            .saturating_add(24)
            .clamp(512 * 1024, 4 * 1024 * 1024);

        let mut buffer_start = index_start_guess;
        let buffer = match self
            .read_range_via_cli_resilient(blob_id, index_start_guess, window_len)
            .await
        {
            Ok(buf) => buf,
            Err(e) => {
                if let Some(actual_size) = Self::parse_blob_size_from_error(&e) {
                    if actual_size <= index_start_guess {
                        return Err(anyhow::anyhow!(
                            "blob size {} is before index start {}",
                            actual_size,
                            index_start_guess
                        ));
                    }
                    let tail_len = (256 * 1024).min(actual_size);
                    let tail_start = actual_size - tail_len;
                    buffer_start = tail_start;
                    self.read_range_via_cli_resilient(blob_id, tail_start, tail_len)
                        .await?
                } else {
                    return Err(e);
                }
            }
        };

        let magic_bytes = 0x574c4244u32.to_le_bytes();
        for i in (0..buffer.len().saturating_sub(24)).rev() {
            if buffer[i..i + 4] == magic_bytes {
                let footer = &buffer[i..i + 24];
                let mut cursor = Cursor::new(footer);
                let magic = cursor.read_u32::<LittleEndian>()?;
                if magic != 0x574c4244 {
                    continue;
                }
                let _version = cursor.read_u32::<LittleEndian>()?;
                let index_start_offset = cursor.read_u64::<LittleEndian>()?;
                let _count = cursor.read_u32::<LittleEndian>()?;
                if index_start_offset == index_start_guess {
                    let total_size = buffer_start + i as u64 + 24;
                    return Ok((total_size, footer.to_vec()));
                }
            }
        }

        Err(anyhow::anyhow!(
            "failed to locate footer within index window (guess={}, window_len={})",
            index_start_guess,
            window_len
        ))
    }

    /// Download a specific range from a blob (HTTP or Local File)
    async fn download_range(&self, blob_id: &str, start: u64, length: u64) -> Result<Vec<u8>> {
        // 1. Check if we have the file locally (or should force download it via CLI)
        let cached_path = self.get_cached_blob_path(blob_id);
        
        // If configured to use CLI, either stream the range or ensure the blob is cached
        if self.inner.walrus_cli_path.is_some() {
            if self.inner.cache_enabled {
                // This is a heavy operation (downloading full blob), but guarantees availability
                // Only performed once per blob
                if !cached_path.exists() {
                    self.download_blob_via_cli(blob_id).await?;
                }
            } else {
                return self.read_range_via_cli_resilient(blob_id, start, length).await;
            }
        }

        // 2. If local file exists, read from it
        if cached_path.exists() {
            // tracing::debug!("reading range {}-{} from local file {}", start, start + length, cached_path.display());
            let mut file = std::fs::File::open(&cached_path)
                .with_context(|| format!("failed to open cached blob {}", cached_path.display()))?;
            
            file.seek(SeekFrom::Start(start))?;
            let mut buffer = vec![0u8; length as usize];
            file.read_exact(&mut buffer)?;
            
            return Ok(buffer);
        }

        // 3. Fallback to HTTP Range request (Aggregator)
        let url = format!("{}/v1/blobs/{}", self.inner.aggregator_url, blob_id);
        let end = start + length - 1;
        
        tracing::debug!("downloading range {}-{} (len {}) from {}", start, end, length, blob_id);

        let bytes = self.with_retry(|| {
            let client = self.inner.client.clone();
            let url = url.clone();
            async move {
                let response = client.get(&url)
                    .header("Range", format!("bytes={}-{}", start, end))
                    .send()
                    .await?;

                if !response.status().is_success() {
                     return Err(anyhow::anyhow!("status {}", response.status()));
                }

                Ok(response.bytes().await?.to_vec())
            }
        }).await?;

        if bytes.len() as u64 != length {
             tracing::warn!("expected {} bytes, got {}", length, bytes.len());
        }

        self.inner.bytes_downloaded.fetch_add(bytes.len() as u64, Ordering::Relaxed);
        Ok(bytes)
    }

    /// Download checkpoints to a local directory as .chk files
    /// Files contain the raw BCS bytes of the CheckpointData
    pub async fn download_checkpoints_to_dir(
        &self,
        range: std::ops::Range<CheckpointSequenceNumber>,
        output_dir: PathBuf,
    ) -> Result<()> {
        tokio::fs::create_dir_all(&output_dir).await?;

        let metadata = self.inner.metadata.read().await;
        // Find all needed blobs
        let blobs: Vec<BlobMetadata> = metadata.iter()
            .filter(|b| b.end_checkpoint >= range.start && b.start_checkpoint < range.end)
            .cloned()
            .collect();
        drop(metadata); // Release lock

        if blobs.is_empty() {
            return Ok(());
        }

        // Process blobs in parallel
        // If using CLI, we can handle multiple heavy downloads.
        // If using Aggregator, we process blobs one-by-one (chunking) to avoid timeouts.
        let blob_concurrency = if self.inner.walrus_cli_path.is_some() { 3 } else { 1 };
        
        let mut blob_stream = stream::iter(blobs)
            .map(|blob| {
                let storage = self.clone();
                let output_dir = output_dir.clone();
                let range = range.clone();
                
                async move {
                    // If using CLI with cache enabled, ensure blob is downloaded once
                    if storage.inner.walrus_cli_path.is_some() && storage.inner.cache_enabled {
                        storage.download_blob_via_cli(&blob.blob_id).await?;
                    }

                    // Load index
                    let index = match storage.load_blob_index(&blob.blob_id).await {
                        Ok(index) => index,
                        Err(e) if storage.inner.walrus_cli_path.is_some() => {
                            storage.mark_bad_blob(&blob.blob_id).await;
                            tracing::warn!(
                                "skipping blob {} due to index load error: {}",
                                blob.blob_id,
                                e
                            );
                            return Ok::<(), anyhow::Error>(());
                        }
                        Err(e) => return Err(e),
                    };

                    // Identify checkpoints to fetch
                    let mut tasks = Vec::new();
                    for cp_num in range.start..range.end {
                        if let Some(entry) = index.get(&cp_num) {
                            tasks.push((cp_num, entry.clone()));
                        }
                    }

                    if tasks.is_empty() {
                        return Ok::<(), anyhow::Error>(());
                    }

                    tracing::info!("downloading {} checkpoints from blob {} to {}", tasks.len(), blob.blob_id, output_dir.display());

                    let start_extract = std::time::Instant::now();

                    // Process checkpoints in chunks if using Aggregator
                    // This prevents overwhelming the proxy with too many concurrent requests per blob
                    let chunk_size = if storage.inner.walrus_cli_path.is_some() { tasks.len() } else { 200 };
                    let mut count = 0;

                    let max_gap_bytes = storage.inner.coalesce_gap_bytes;
                    let max_range_bytes = if storage.inner.walrus_cli_path.is_some() {
                        storage.inner.coalesce_max_range_bytes_cli
                    } else {
                        storage.inner.coalesce_max_range_bytes
                    };

                    for chunk in tasks.chunks(chunk_size) {
                        let pending: Vec<(CheckpointSequenceNumber, BlobIndexEntry, PathBuf)> =
                            stream::iter(chunk.to_vec())
                                .filter_map(|(cp_num, entry)| {
                                    let file_path = output_dir.join(format!("{}.chk", cp_num));
                                    async move {
                                        if tokio::fs::try_exists(&file_path)
                                            .await
                                            .unwrap_or(false)
                                        {
                                            None
                                        } else {
                                            Some((cp_num, entry, file_path))
                                        }
                                    }
                                })
                                .collect()
                                .await;

                        if pending.is_empty() {
                            continue;
                        }

                        let coalesced = coalesce_entries(&pending, max_gap_bytes, max_range_bytes);

                        for range in coalesced {
                            if range.len() == 0 {
                                continue;
                            }

                            let bytes = storage
                                .download_range(&blob.blob_id, range.start, range.len())
                                .await?;

                            for (_cp_num, entry, file_path) in range.entries {
                                let start = entry.offset.saturating_sub(range.start) as usize;
                                let end = start + entry.length as usize;
                                tokio::fs::write(&file_path, &bytes[start..end]).await?;
                                count += 1;
                            }
                        }
                    }

                    let elapsed_extract = start_extract.elapsed();
                    if count > 0 {
                        tracing::info!("extracted {} checkpoints from blob {} in {:.2}s ({:.2} cp/s)", 
                            count, 
                            blob.blob_id, 
                            elapsed_extract.as_secs_f64(),
                            count as f64 / elapsed_extract.as_secs_f64()
                        );
                    }
                    Ok(())
                }
            })
            .buffer_unordered(blob_concurrency);

        while let Some(result) = blob_stream.next().await {
            result?;
        }
        
        Ok(())
    }

    /// Scan a checkpoint range for package-related events/objects and build a simple index.
    /// This is intended for ad-hoc analysis and is not wired into the CLI yet.
    pub async fn build_package_checkpoint_index(
        &self,
        range: std::ops::Range<CheckpointSequenceNumber>,
        package_id: ObjectID,
        output_path: Option<PathBuf>,
    ) -> Result<PackageCheckpointIndex> {
        let metadata = self.inner.metadata.read().await;
        let blobs: Vec<BlobMetadata> = metadata
            .iter()
            .filter(|b| b.end_checkpoint >= range.start && b.start_checkpoint < range.end)
            .cloned()
            .collect();
        drop(metadata);

        if blobs.is_empty() {
            return Ok(PackageCheckpointIndex {
                package_id,
                hits: Vec::new(),
            });
        }

        let hits = Arc::new(RwLock::new(Vec::new()));
        let blob_concurrency = if self.inner.walrus_cli_path.is_some() { 3 } else { 1 };

        let mut blob_stream = stream::iter(blobs)
            .map(|blob| {
                let storage = self.clone();
                let range = range.clone();
                let hits = hits.clone();
                let package_id = package_id;

                async move {
                    if storage.inner.walrus_cli_path.is_some() && storage.inner.cache_enabled {
                        storage.download_blob_via_cli(&blob.blob_id).await?;
                    }

                    let index = match storage.load_blob_index(&blob.blob_id).await {
                        Ok(index) => index,
                        Err(e) if storage.inner.walrus_cli_path.is_some() => {
                            storage.mark_bad_blob(&blob.blob_id).await;
                            tracing::warn!(
                                "skipping blob {} due to index load error: {}",
                                blob.blob_id,
                                e
                            );
                            return Ok::<(), anyhow::Error>(());
                        }
                        Err(e) => return Err(e),
                    };

                    let mut tasks = Vec::new();
                    for cp_num in range.start..range.end {
                        if let Some(entry) = index.get(&cp_num) {
                            tasks.push((cp_num, entry.clone()));
                        }
                    }

                    if tasks.is_empty() {
                        return Ok::<(), anyhow::Error>(());
                    }

                    let chunk_size = if storage.inner.walrus_cli_path.is_some() {
                        tasks.len()
                    } else {
                        200
                    };

                    let max_gap_bytes = storage.inner.coalesce_gap_bytes;
                    let max_range_bytes = if storage.inner.walrus_cli_path.is_some() {
                        storage.inner.coalesce_max_range_bytes_cli
                    } else {
                        storage.inner.coalesce_max_range_bytes
                    };

                    let mut local_hits = Vec::new();

                    for chunk in tasks.chunks(chunk_size) {
                        let pending: Vec<(CheckpointSequenceNumber, BlobIndexEntry, ())> =
                            chunk.iter().cloned().map(|(cp_num, entry)| (cp_num, entry, ())).collect();

                        let coalesced = coalesce_entries(&pending, max_gap_bytes, max_range_bytes);

                        for range in coalesced {
                            if range.len() == 0 {
                                continue;
                            }

                            let bytes = storage
                                .download_range(&blob.blob_id, range.start, range.len())
                                .await?;

                            for (cp_num, entry, _) in range.entries {
                                let start = entry.offset.saturating_sub(range.start) as usize;
                                let end = start + entry.length as usize;
                                let checkpoint = sui_storage::blob::Blob::from_bytes::<CheckpointData>(
                                    &bytes[start..end],
                                )
                                .with_context(|| format!("failed to deserialize checkpoint {}", cp_num))?;

                                let (event_count, object_count) =
                                    count_package_hits(&checkpoint, package_id);

                                if event_count > 0 || object_count > 0 {
                                    local_hits.push(PackageCheckpointHit {
                                        checkpoint: cp_num,
                                        event_count,
                                        object_count,
                                    });
                                }
                            }
                        }
                    }

                    if !local_hits.is_empty() {
                        let mut lock = hits.write().await;
                        lock.extend(local_hits);
                    }

                    Ok(())
                }
            })
            .buffer_unordered(blob_concurrency);

        while let Some(result) = blob_stream.next().await {
            result?;
        }

        drop(blob_stream);

        let mut final_hits = Arc::try_unwrap(hits).unwrap().into_inner();
        final_hits.sort_by_key(|hit| hit.checkpoint);

        let index = PackageCheckpointIndex {
            package_id,
            hits: final_hits,
        };

        if let Some(path) = output_path {
            let data = serde_json::to_vec_pretty(&index)?;
            tokio::fs::write(path, data).await?;
        }

        Ok(index)
    }
}

#[async_trait]
impl CheckpointStorage for WalrusCheckpointStorage {
    async fn get_checkpoint(
        &self,
        checkpoint: CheckpointSequenceNumber,
    ) -> Result<CheckpointData> {
        // Find blob containing this checkpoint
        let blob = self
            .find_blob_for_checkpoint(checkpoint)
            .await
            .ok_or_else(|| anyhow::anyhow!("no blob found for checkpoint {}", checkpoint))?;

        // Load index (cached or fetch)
        let index = self.load_blob_index(&blob.blob_id).await?;

        // Find checkpoint in index
        let entry = index.get(&checkpoint)
            .ok_or_else(|| anyhow::anyhow!("checkpoint {} not found in blob index", checkpoint))?;

        // Download checkpoint data
        let cp_bytes = self.download_range(&blob.blob_id, entry.offset, entry.length).await?;

        // Deserialize
        let checkpoint = sui_storage::blob::Blob::from_bytes::<CheckpointData>(&cp_bytes)
            .with_context(|| format!("failed to deserialize checkpoint {}", checkpoint))?;

        Ok(checkpoint)
    }

    async fn get_checkpoints(
        &self,
        range: std::ops::Range<CheckpointSequenceNumber>,
    ) -> Result<Vec<CheckpointData>> {
        let metadata: tokio::sync::RwLockReadGuard<Vec<BlobMetadata>> = self.inner.metadata.read().await;
        // Find all needed blobs
        let blobs: Vec<BlobMetadata> = metadata.iter()
            .filter(|b| b.end_checkpoint >= range.start && b.start_checkpoint < range.end)
            .cloned()
            .collect();
        drop(metadata);

        if blobs.is_empty() {
            return Ok(Vec::new());
        }

        let checkpoints = Arc::new(RwLock::new(Vec::new()));

        // Process blobs in parallel
        let blob_concurrency = if self.inner.walrus_cli_path.is_some() {
            self.inner.walrus_cli_blob_concurrency
        } else {
            1
        };
        
        let mut blob_stream = stream::iter(blobs)
            .map(|blob| {
                let storage = self.clone();
                let range = range.clone();
                let checkpoints = checkpoints.clone();
                
                async move {
                    // If using CLI, ensure blob is downloaded ONCE before parsing index
                    if storage.inner.walrus_cli_path.is_some() && storage.inner.cache_enabled {
                        storage.download_blob_via_cli(&blob.blob_id).await?;
                    }

                    // Load index (cached or fetch - light operation)
                    let index = match storage.load_blob_index(&blob.blob_id).await {
                        Ok(index) => index,
                        Err(e) if storage.inner.walrus_cli_path.is_some() => {
                            storage.mark_bad_blob(&blob.blob_id).await;
                            tracing::warn!(
                                "skipping blob {} due to index load error: {}",
                                blob.blob_id,
                                e
                            );
                            return Ok::<(), anyhow::Error>(());
                        }
                        Err(e) => return Err(e),
                    };

                    // Identify checkpoints to fetch from this blob
                    let mut tasks = Vec::new();
                    for cp_num in range.start..range.end {
                        if let Some(entry) = index.get(&cp_num) {
                            tasks.push((cp_num, entry.clone()));
                        }
                    }

                    if tasks.is_empty() {
                        return Ok::<(), anyhow::Error>(());
                    }

                    tracing::debug!("downloading {} checkpoints from blob {}", tasks.len(), blob.blob_id);

                    // Process in chunks for Aggregator
                    let chunk_size = if storage.inner.walrus_cli_path.is_some() { tasks.len() } else { 200 };
                    let mut results = Vec::new();

                    let max_gap_bytes = storage.inner.coalesce_gap_bytes;
                    let max_range_bytes = if storage.inner.walrus_cli_path.is_some() {
                        storage.inner.coalesce_max_range_bytes_cli
                    } else {
                        storage.inner.coalesce_max_range_bytes
                    };

                    for chunk in tasks.chunks(chunk_size) {
                        let pending: Vec<(CheckpointSequenceNumber, BlobIndexEntry, ())> =
                            chunk.iter().cloned().map(|(cp_num, entry)| (cp_num, entry, ())).collect();

                        let coalesced = coalesce_entries(&pending, max_gap_bytes, max_range_bytes);
                        let range_concurrency = if storage.inner.walrus_cli_path.is_some() {
                            storage.inner.walrus_cli_range_concurrency
                        } else {
                            1
                        };

                        let mut range_stream = stream::iter(coalesced)
                            .map(|range| {
                                let storage = storage.clone();
                                let blob_id = blob.blob_id.clone();
                                async move {
                                    if range.len() == 0 {
                                        return Ok::<Vec<CheckpointData>, anyhow::Error>(Vec::new());
                                    }

                                    let bytes = storage
                                        .download_range(&blob_id, range.start, range.len())
                                        .await?;

                                    let mut decoded = Vec::with_capacity(range.entries.len());
                                    for (cp_num, entry, _) in range.entries {
                                        let start = entry.offset.saturating_sub(range.start) as usize;
                                        let end = start + entry.length as usize;
                                        let checkpoint = sui_storage::blob::Blob::from_bytes::<CheckpointData>(
                                            &bytes[start..end],
                                        )
                                        .with_context(|| format!("failed to deserialize checkpoint {}", cp_num))?;
                                        decoded.push(checkpoint);
                                    }
                                    Ok(decoded)
                                }
                            })
                            .buffer_unordered(range_concurrency);

                        while let Some(range_result) = range_stream.next().await {
                            let mut decoded = range_result?;
                            results.append(&mut decoded);
                        }
                    }

                    // Add to shared collection
                    let mut lock = checkpoints.write().await;
                    lock.extend(results);
                    Ok(())
                }
            })
            .buffer_unordered(blob_concurrency);

        while let Some(result) = blob_stream.next().await {
            result?;
        }
        
        drop(blob_stream);

        // Final collection and sorting
        let mut final_checkpoints = Arc::try_unwrap(checkpoints).unwrap().into_inner();
        final_checkpoints.sort_by_key(|cp| cp.checkpoint_summary.sequence_number);
        
        Ok(final_checkpoints)
    }


    async fn has_checkpoint(
        &self,
        checkpoint: CheckpointSequenceNumber,
    ) -> Result<bool> {
        // Check if any blob contains this checkpoint
        Ok(self.find_blob_for_checkpoint(checkpoint).await.is_some())
    }

    async fn get_latest_checkpoint(&self) -> Result<Option<CheckpointSequenceNumber>> {
        let metadata: tokio::sync::RwLockReadGuard<Vec<BlobMetadata>> = self.inner.metadata.read().await;
        // Find highest checkpoint in blob metadata
        metadata
            .iter()
            .map(|blob| blob.end_checkpoint)
            .max()
            .map(|cp| Ok::<CheckpointSequenceNumber, anyhow::Error>(cp))
            .transpose()
            .context("failed to find latest checkpoint in Walrus blobs")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use move_core_types::account_address::AccountAddress;
    use move_core_types::identifier::Identifier;
    use move_core_types::language_storage::{StructTag, TypeTag};
    use move_core_types::{ident_str};
    use sui_types::base_types::{ObjectID, SuiAddress};
    use sui_types::event::Event;
    use sui_types::full_checkpoint_content::CheckpointData;
    use sui_types::test_checkpoint_data_builder::TestCheckpointBuilder;

    #[test]
    fn coalesce_entries_merges_adjacent() {
        let entries = vec![
            (1u64, BlobIndexEntry { checkpoint_number: 1, offset: 0, length: 100 }, ()),
            (2u64, BlobIndexEntry { checkpoint_number: 2, offset: 100, length: 50 }, ()),
            (3u64, BlobIndexEntry { checkpoint_number: 3, offset: 200, length: 25 }, ()),
        ];

        let ranges = coalesce_entries(&entries, 0, 1024);
        assert_eq!(ranges.len(), 2);
        assert_eq!(ranges[0].start, 0);
        assert_eq!(ranges[0].end, 150);
        assert_eq!(ranges[0].entries.len(), 2);
        assert_eq!(ranges[1].start, 200);
        assert_eq!(ranges[1].end, 225);
    }

    #[test]
    fn coalesce_entries_respects_max_range() {
        let entries = vec![
            (1u64, BlobIndexEntry { checkpoint_number: 1, offset: 0, length: 100 }, ()),
            (2u64, BlobIndexEntry { checkpoint_number: 2, offset: 100, length: 100 }, ()),
            (3u64, BlobIndexEntry { checkpoint_number: 3, offset: 200, length: 100 }, ()),
        ];

        let ranges = coalesce_entries(&entries, 0, 150);
        assert_eq!(ranges.len(), 3);
        assert_eq!(ranges[0].start, 0);
        assert_eq!(ranges[0].end, 100);
        assert_eq!(ranges[1].start, 100);
        assert_eq!(ranges[1].end, 200);
        assert_eq!(ranges[2].start, 200);
        assert_eq!(ranges[2].end, 300);
    }

    #[test]
    fn count_package_hits_counts_events_and_objects() {
        let package_id = ObjectID::from_single_byte(7);
        let sender = SuiAddress::from(ObjectID::from_single_byte(9));
        let package_address = AccountAddress::from(package_id);

        let event = Event::new(
            &package_address,
            ident_str!("mod"),
            sender,
            StructTag {
                address: package_address,
                module: Identifier::new("mod").unwrap(),
                name: Identifier::new("Evt").unwrap(),
                type_params: vec![],
            },
            vec![1, 2, 3],
        );

        let coin_type = TypeTag::Struct(Box::new(StructTag {
            address: package_address,
            module: Identifier::new("coin").unwrap(),
            name: Identifier::new("Coin").unwrap(),
            type_params: vec![],
        }));

        let mut builder = TestCheckpointBuilder::new(0)
            .start_transaction(0)
            .with_events(vec![event])
            .create_coin_object(1, 0, 100, coin_type)
            .finish_transaction();

        let checkpoint = builder.build_checkpoint();
        let checkpoint_data = CheckpointData::from(checkpoint);

        let (event_count, object_count) = count_package_hits(&checkpoint_data, package_id);
        assert_eq!(event_count, 1);
        assert_eq!(object_count, 0);
    }
}
