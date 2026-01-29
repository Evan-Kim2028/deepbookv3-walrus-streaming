// Copyright (c) DeepBook V3. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use anyhow::{Context, Result};
use byteorder::{LittleEndian, ReadBytesExt};
use clap::{Parser, ValueEnum};
use futures::{stream, StreamExt};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap};
use std::io::{Cursor, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::str::FromStr;
use std::time::Instant;
use sui_types::base_types::ObjectID;
use sui_types::full_checkpoint_content::CheckpointData;
use sui_storage::blob::{Blob, BlobEncoding};
use tokio::io::AsyncRead;
use tokio::process::Command;
use tokio::sync::Mutex;

#[derive(Debug, Clone, Deserialize)]
struct BlobMetadata {
    #[serde(rename = "blob_id")]
    blob_id: String,
    #[serde(rename = "start_checkpoint")]
    start_checkpoint: u64,
    #[serde(rename = "end_checkpoint")]
    end_checkpoint: u64,
    #[serde(rename = "entries_count")]
    entries_count: u64,
    total_size: u64,
    #[serde(default)]
    #[allow(dead_code)]
    end_of_epoch: bool,
    #[serde(default)]
    #[allow(dead_code)]
    expiry_epoch: u64,
}

#[derive(Debug, Deserialize)]
struct BlobsResponse {
    blobs: Vec<BlobMetadata>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct BlobIndexEntry {
    #[allow(dead_code)]
    checkpoint_number: u64,
    offset: u64,
    length: u64,
}

#[derive(Debug, Clone)]
struct RangePlan {
    start: u64,
    end: u64,
    entries: Vec<(u64, BlobIndexEntry)>,
}

impl RangePlan {
    fn len(&self) -> u64 {
        self.end.saturating_sub(self.start)
    }
}

#[derive(Debug, Clone)]
struct RangeWork {
    idx: u64,
    range: RangePlan,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Transport {
    /// Stream blob bytes from Walrus CLI (no HTTP range reads)
    Cli,
    /// Stream blob bytes via HTTP range requests (uses aggregator)
    Http,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum IndexSource {
    /// Build index by streaming the blob via Walrus CLI (no aggregator)
    Cli,
    /// Load index from a JSON file on disk
    File,
    /// Do not load an index (raw streaming only)
    None,
    /// Fetch index via Walrus aggregator (HTTP)
    Aggregator,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum DecodeMode {
    /// Decode checkpoints by using the blob index (random access ranges)
    Index,
    /// Decode checkpoints sequentially from the stream (length-prefixed blobs)
    Sequential,
}

#[derive(Debug, Clone, Parser)]
#[command(author, version, about = "Walrus blob streaming experiments (checkpoint decode)")]
struct Args {
    /// Walrus archival service URL (for blob metadata)
    #[arg(long, default_value = "https://walrus-sui-archival.mainnet.walrus.space")]
    archival_url: String,

    /// Walrus aggregator URL (only used for HTTP transport)
    #[arg(long, default_value = "https://aggregator.walrus-mainnet.walrus.space")]
    aggregator_url: String,

    /// Specific blob ID to target (defaults to latest)
    #[arg(long)]
    blob_id: Option<String>,

    /// Number of latest blobs to process (ignored if --blob-id is set)
    #[arg(long, default_value_t = 1)]
    blob_count: u64,

    /// Max number of blobs to stream in parallel
    #[arg(long, default_value_t = 1)]
    concurrency: usize,

    /// Package ID to scan for (optional)
    #[arg(long)]
    package_id: Option<String>,

    /// Start checkpoint (inclusive)
    #[arg(long)]
    start_checkpoint: Option<u64>,

    /// End checkpoint (inclusive)
    #[arg(long)]
    end_checkpoint: Option<u64>,

    /// Limit number of checkpoints (used with --from-latest or --start-checkpoint)
    #[arg(long, default_value_t = 0)]
    limit: u64,

    /// Start from latest checkpoint in the blob
    #[arg(long, default_value_t = false)]
    from_latest: bool,

    /// Transport for blob data (http or cli)
    #[arg(long, value_enum, default_value_t = Transport::Cli)]
    transport: Transport,

    /// Index source (cli, file, none, or aggregator)
    #[arg(long, value_enum, default_value_t = IndexSource::Cli)]
    index_source: IndexSource,

    /// Decode mode (index = use blob index, sequential = stream decode)
    #[arg(long, value_enum, default_value_t = DecodeMode::Index)]
    decode_mode: DecodeMode,

    /// Path to index JSON file (required for --index-source file)
    #[arg(long)]
    index_file: Option<PathBuf>,

    /// Write extracted index JSON to this path
    #[arg(long)]
    index_out: Option<PathBuf>,

    /// Path to walrus CLI (required for --transport cli)
    #[arg(long)]
    walrus_cli_path: Option<PathBuf>,

    /// Walrus CLI config path (optional)
    #[arg(long)]
    walrus_config: Option<PathBuf>,

    /// Walrus CLI context (optional, e.g., mainnet)
    #[arg(long, default_value = "mainnet")]
    walrus_context: String,

    /// HTTP timeout in seconds
    #[arg(long, default_value_t = 60)]
    http_timeout_secs: u64,

    /// Range coalescing gap in bytes (0 = contiguous only)
    #[arg(long, default_value_t = 0)]
    coalesce_gap_bytes: u64,

    /// Max coalesced range size in bytes (0 = unlimited)
    #[arg(long, default_value_t = 33554432)]
    max_range_bytes: u64,

    /// Max number of concurrent CLI range reads per blob
    #[arg(long, default_value_t = 1)]
    range_concurrency: usize,

    /// Skip Walrus consistency checks (faster, less verification)
    #[arg(long, default_value_t = false)]
    skip_consistency_check: bool,

    /// Force a single contiguous range window covering all selected checkpoints
    #[arg(long, default_value_t = false)]
    single_window: bool,

    /// Stream CLI ranges and decode on the fly (lower memory, single-pass)
    #[arg(long, default_value_t = false)]
    stream_ranges: bool,

    /// Path to a resume state JSON file (enables resume across restarts)
    #[arg(long)]
    resume_path: Option<PathBuf>,

    /// Chunk the blob into N ranges (e.g., 2 = halves). Overrides coalescing.
    #[arg(long, default_value_t = 0)]
    chunk_count: u64,

    /// Process only a single chunk index (0-based). Requires --chunk-count.
    #[arg(long)]
    chunk_index: Option<u64>,

    /// Raw download mode: stream ranges and discard (no decoding)
    #[arg(long, default_value_t = false)]
    raw_download: bool,

    /// Override index tail buffer size in bytes when using --index-source cli
    #[arg(long, default_value_t = 0)]
    index_buffer_bytes: u64,
}

struct Stats {
    ranges: u64,
    checkpoints: u64,
    events: u64,
    objects: u64,
    bytes: u64,
    started_at: Instant,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct ResumeState {
    blobs: HashMap<String, BlobResume>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct BlobResume {
    completed_ranges: BTreeSet<u64>,
    completed: bool,
}

#[derive(Debug)]
struct ResumeHandle {
    path: PathBuf,
    state: Mutex<ResumeState>,
}

impl ResumeHandle {
    async fn load(path: PathBuf) -> Result<Self> {
        let state = if tokio::fs::try_exists(&path).await.unwrap_or(false) {
            let data = tokio::fs::read(&path)
                .await
                .with_context(|| format!("failed to read resume file {}", path.display()))?;
            serde_json::from_slice(&data)
                .with_context(|| format!("failed to parse resume file {}", path.display()))?
        } else {
            ResumeState::default()
        };

        Ok(Self {
            path,
            state: Mutex::new(state),
        })
    }

    async fn completed_ranges(&self, blob_id: &str) -> BTreeSet<u64> {
        let state = self.state.lock().await;
        state
            .blobs
            .get(blob_id)
            .map(|blob| blob.completed_ranges.clone())
            .unwrap_or_default()
    }

    async fn is_blob_complete(&self, blob_id: &str) -> bool {
        let state = self.state.lock().await;
        state
            .blobs
            .get(blob_id)
            .map(|blob| blob.completed)
            .unwrap_or(false)
    }

    async fn mark_completed(&self, blob_id: &str, idx: u64) -> Result<()> {
        let mut state = self.state.lock().await;
        let blob = state
            .blobs
            .entry(blob_id.to_string())
            .or_insert_with(BlobResume::default);
        if !blob.completed_ranges.insert(idx) {
            return Ok(());
        }

        let data = serde_json::to_vec_pretty(&*state)?;
        tokio::fs::write(&self.path, data)
            .await
            .with_context(|| format!("failed to write resume file {}", self.path.display()))?;
        Ok(())
    }

    async fn mark_blob_complete(&self, blob_id: &str) -> Result<()> {
        let mut state = self.state.lock().await;
        let blob = state
            .blobs
            .entry(blob_id.to_string())
            .or_insert_with(BlobResume::default);
        if blob.completed {
            return Ok(());
        }
        blob.completed = true;
        let data = serde_json::to_vec_pretty(&*state)?;
        tokio::fs::write(&self.path, data)
            .await
            .with_context(|| format!("failed to write resume file {}", self.path.display()))?;
        Ok(())
    }
}

impl Stats {
    fn new() -> Self {
        Self {
            ranges: 0,
            checkpoints: 0,
            events: 0,
            objects: 0,
            bytes: 0,
            started_at: Instant::now(),
        }
    }

    fn with_start(started_at: Instant) -> Self {
        Self {
            started_at,
            ..Self::new()
        }
    }

    fn merge(&mut self, other: &Stats) {
        self.ranges += other.ranges;
        self.checkpoints += other.checkpoints;
        self.events += other.events;
        self.objects += other.objects;
        self.bytes += other.bytes;
    }

    fn report(&self) {
        let elapsed = self.started_at.elapsed().as_secs_f64().max(0.001);
        let mb = self.bytes as f64 / 1_000_000.0;
        let mbps = mb / elapsed;
        eprintln!(
            "done: ranges={}, checkpoints={}, events={}, objects={}, bytes={:.2} MB, {:.2} MB/s",
            self.ranges, self.checkpoints, self.events, self.objects, mb, mbps
        );
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    if matches!(args.transport, Transport::Cli) && args.walrus_cli_path.is_none() {
        return Err(anyhow::anyhow!("--transport cli requires --walrus-cli-path"));
    }

    if matches!(args.index_source, IndexSource::File) && args.index_file.is_none() {
        return Err(anyhow::anyhow!("--index-source file requires --index-file"));
    }

    if matches!(args.transport, Transport::Cli) && matches!(args.index_source, IndexSource::Aggregator) {
        return Err(anyhow::anyhow!(
            "CLI transport cannot use aggregator index (no hybrid). Use --index-source cli or file."
        ));
    }

    if matches!(args.transport, Transport::Http) && matches!(args.index_source, IndexSource::Cli) {
        return Err(anyhow::anyhow!(
            "HTTP transport cannot use CLI index source. Use --index-source aggregator or file."
        ));
    }

    if args.chunk_index.is_some() && args.chunk_count == 0 {
        return Err(anyhow::anyhow!(
            "--chunk-index requires --chunk-count to be set"
        ));
    }

    if args.package_id.is_some()
        && (matches!(args.index_source, IndexSource::None) || args.raw_download)
        && !matches!(args.decode_mode, DecodeMode::Sequential)
    {
        return Err(anyhow::anyhow!(
            "package scanning requires an index; disable --raw-download and use --index-source cli or file"
        ));
    }

    if args.stream_ranges && !matches!(args.transport, Transport::Cli) {
        return Err(anyhow::anyhow!(
            "--stream-ranges requires --transport cli"
        ));
    }

    if matches!(args.decode_mode, DecodeMode::Sequential)
        && !matches!(args.transport, Transport::Cli)
    {
        return Err(anyhow::anyhow!(
            "sequential decode requires CLI transport"
        ));
    }

    let resume = match args.resume_path.clone() {
        Some(path) => Some(Arc::new(ResumeHandle::load(path).await?)),
        None => None,
    };

    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(args.http_timeout_secs))
        .build()
        .context("failed to create HTTP client")?;

    let blobs = fetch_blobs(&client, &args.archival_url).await?;
    let selected = select_blobs(&blobs, args.blob_id.as_deref(), args.blob_count)?;

    if selected.len() > 1 && matches!(args.index_source, IndexSource::File) {
        return Err(anyhow::anyhow!(
            "--index-source file supports only a single blob per run"
        ));
    }

    let package_id = match args.package_id {
        Some(ref s) => Some(ObjectID::from_str(s).context("invalid package id")?),
        None => None,
    };

    let started_at = Instant::now();
    let multi_blob = selected.len() > 1;
    let mut total = Stats::with_start(started_at);

    let blob_stream = futures::stream::iter(selected)
        .map(|blob| {
            let args = args.clone();
            let client = client.clone();
            let resume = resume.clone();
            async move {
                process_blob(&client, &args, blob, package_id, multi_blob, resume).await
            }
        })
        .buffer_unordered(args.concurrency.max(1));

    tokio::pin!(blob_stream);

    while let Some(result) = blob_stream.next().await {
        let stats = result?;
        total.merge(&stats);
    }

    total.report();
    Ok(())
}

fn select_blobs(
    blobs: &[BlobMetadata],
    blob_id: Option<&str>,
    blob_count: u64,
) -> Result<Vec<BlobMetadata>> {
    if let Some(blob_id) = blob_id {
        let blob = blobs
            .iter()
            .find(|blob| blob.blob_id == blob_id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("blob {} not found", blob_id))?;
        return Ok(vec![blob]);
    }

    if blobs.is_empty() {
        return Err(anyhow::anyhow!("no blob metadata returned"));
    }

    let mut sorted = blobs.to_vec();
    sorted.sort_by_key(|blob| std::cmp::Reverse(blob.end_checkpoint));
    let count = blob_count.max(1) as usize;
    Ok(sorted.into_iter().take(count).collect())
}

async fn process_blob(
    client: &Client,
    args: &Args,
    blob: BlobMetadata,
    package_id: Option<ObjectID>,
    multi_blob: bool,
    resume: Option<Arc<ResumeHandle>>,
) -> Result<Stats> {
    eprintln!(
        "using blob {} checkpoints {}..{} (entries={}, size={} bytes)",
        blob.blob_id,
        blob.start_checkpoint,
        blob.end_checkpoint,
        blob.entries_count,
        blob.total_size
    );

    if let Some(resume) = &resume {
        if resume.is_blob_complete(&blob.blob_id).await {
            eprintln!("blob {} already completed; skipping", blob.blob_id);
            return Ok(Stats::new());
        }
    }

    if args.raw_download {
        return run_raw_download(client, args, &blob).await;
    }

    if matches!(args.decode_mode, DecodeMode::Sequential) {
        let cli_path = args
            .walrus_cli_path
            .as_ref()
            .expect("cli path checked above");
        return process_blob_sequential(cli_path, args, &blob, package_id).await;
    }

    if matches!(args.index_source, IndexSource::None) {
        return Err(anyhow::anyhow!(
            "index source none cannot be used with index decode mode"
        ));
    }

    let index = match args.index_source {
        IndexSource::Cli => {
            let cli_path = args
                .walrus_cli_path
                .as_ref()
                .expect("cli path checked above");
            let index =
                extract_index_via_cli(cli_path, args, &blob, args.index_buffer_bytes).await?;
            if let Some(out_path) = &args.index_out {
                let path = index_out_path(out_path, &blob.blob_id, multi_blob);
                let data = serde_json::to_vec_pretty(&index)?;
                tokio::fs::write(path, data).await?;
            }
            index
        }
        IndexSource::File => {
            let path = args.index_file.as_ref().expect("checked above");
            load_index_from_file(path).await?
        }
        IndexSource::Aggregator => {
            fetch_index(client, &args.aggregator_url, &blob.blob_id).await?
        }
        IndexSource::None => {
            return Err(anyhow::anyhow!(
                "index source none cannot be used for decoding"
            ));
        }
    };

    let (range_start, range_end) = resolve_checkpoint_range(args, &blob)?;
    let mut entries = filter_entries(&index, range_start, range_end);

    if entries.is_empty() {
        eprintln!("no checkpoints found in requested range");
        return Ok(Stats::new());
    }

    entries.sort_by_key(|(_, entry)| entry.offset);

    let mut ranges = if args.chunk_count > 0 {
        let strict = args.chunk_index.is_some();
        chunk_entries(&entries, blob.total_size, args.chunk_count, strict)
    } else if args.single_window {
        build_single_window(&entries)?
    } else {
        coalesce_entries(&entries, args.coalesce_gap_bytes, args.max_range_bytes)
    };

    if let Some(chunk_index) = args.chunk_index {
        ranges = select_chunk_range(ranges, chunk_index)?;
    }

    if ranges.is_empty() {
        eprintln!("no ranges to process after coalescing");
        return Ok(Stats::new());
    }

    let mut work_items: Vec<RangeWork> = ranges
        .into_iter()
        .enumerate()
        .map(|(idx, range)| RangeWork {
            idx: idx as u64,
            range,
        })
        .collect();

    if let Some(resume) = &resume {
        let completed = resume.completed_ranges(&blob.blob_id).await;
        work_items.retain(|work| !completed.contains(&work.idx));
    }

    if work_items.is_empty() {
        eprintln!("all ranges already completed for blob {}", blob.blob_id);
        if let Some(resume) = &resume {
            resume.mark_blob_complete(&blob.blob_id).await?;
        }
        return Ok(Stats::new());
    }

    eprintln!("processing {} ranges", work_items.len());

    let mut stats = Stats::new();

    match args.transport {
        Transport::Http => {
            process_ranges_via_http(
                client,
                &args.aggregator_url,
                &blob.blob_id,
                &work_items,
                resume.clone(),
                package_id,
                &mut stats,
            )
            .await?;
        }
        Transport::Cli => {
            let cli_path = args
                .walrus_cli_path
                .as_ref()
                .expect("cli path checked above");
            process_ranges_via_cli(
                cli_path,
                args,
                &blob.blob_id,
                &work_items,
                resume.clone(),
                package_id,
                &mut stats,
            )
            .await?;
        }
    }

    if let Some(resume) = &resume {
        resume.mark_blob_complete(&blob.blob_id).await?;
    }

    Ok(stats)
}

fn index_out_path(base: &PathBuf, blob_id: &str, multi_blob: bool) -> PathBuf {
    if multi_blob {
        if base.is_dir() {
            return base.join(format!("{}.json", blob_id));
        }
        let mut path = base.clone();
        let stem = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("index");
        let ext = path.extension().and_then(|s| s.to_str()).unwrap_or("json");
        path.set_file_name(format!("{}_{}.{}", stem, blob_id, ext));
        return path;
    }

    base.clone()
}

fn select_chunk_range(ranges: Vec<RangePlan>, chunk_index: u64) -> Result<Vec<RangePlan>> {
    let idx = chunk_index as usize;
    if idx >= ranges.len() {
        return Err(anyhow::anyhow!(
            "chunk index {} out of range ({} chunks)",
            chunk_index,
            ranges.len()
        ));
    }
    Ok(vec![ranges[idx].clone()])
}

fn walrus_command(cli_path: &Path, args: &Args) -> Command {
    let mut cmd = Command::new(cli_path);
    if let Some(config) = &args.walrus_config {
        cmd.arg("--config").arg(config);
    }
    cmd.arg("--context").arg(&args.walrus_context);
    cmd
}

fn walrus_read_command(cli_path: &Path, args: &Args) -> Command {
    let mut cmd = walrus_command(cli_path, args);
    cmd.arg("read");
    if args.skip_consistency_check {
        cmd.arg("--skip-consistency-check");
    }
    cmd
}

async fn process_blob_sequential(
    cli_path: &Path,
    args: &Args,
    blob: &BlobMetadata,
    package_id: Option<ObjectID>,
) -> Result<Stats> {
    if blob.entries_count == 0 {
        return Err(anyhow::anyhow!(
            "sequential decode requires entries_count metadata"
        ));
    }

    let (range_start, range_end) = resolve_checkpoint_range(args, blob)?;
    let total_entries = blob.entries_count;

    let mut child = walrus_read_command(cli_path, args)
        .arg(&blob.blob_id)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::inherit())
        .spawn()
        .context("failed to spawn walrus cli")?;

    let mut stdout = child
        .stdout
        .take()
        .context("failed to capture walrus cli stdout")?;

    let mut stats = Stats::new();
    let mut seen = 0u64;

    while seen < total_entries {
        let Some(len) = read_varint_u64(&mut stdout).await? else {
            break;
        };

        if len == 0 {
            return Err(anyhow::anyhow!("invalid zero-length blob entry"));
        }

        let encoding = read_u8(&mut stdout).await?;
        let encoding = BlobEncoding::try_from(encoding)
            .context("unsupported blob encoding")?;

        let len_usize = usize::try_from(len).context("entry length too large")?;
        let mut data = vec![0u8; len_usize];
        tokio::io::AsyncReadExt::read_exact(&mut stdout, &mut data)
            .await
            .context("unexpected EOF while reading entry")?;

        let blob_entry = Blob { data, encoding };
        let checkpoint: CheckpointData = blob_entry
            .decode()
            .with_context(|| format!("failed to decode checkpoint entry {}", seen))?;

        let seq = checkpoint.checkpoint_summary.sequence_number;
        if seq >= range_start && seq < range_end {
            stats.checkpoints += 1;
            if let Some(package_id) = package_id {
                let (events, objects) = count_package_hits(&checkpoint, package_id);
                stats.events += events;
                stats.objects += objects;
            }
        }

        stats.bytes += len + 1; // encoding byte + data length (varint not counted)
        seen += 1;
    }

    let _ = child.kill().await;
    let _ = child.wait().await;

    Ok(stats)
}

async fn read_varint_u64<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Option<u64>> {
    let mut value: u64 = 0;
    let mut shift = 0u32;
    let mut saw_any = false;

    loop {
        let mut buf = [0u8; 1];
        let n = tokio::io::AsyncReadExt::read(reader, &mut buf).await?;
        if n == 0 {
            if saw_any {
                return Err(anyhow::anyhow!("unexpected EOF while reading varint"));
            }
            return Ok(None);
        }

        saw_any = true;
        let byte = buf[0];
        value |= ((byte & 0x7f) as u64) << shift;

        if byte & 0x80 == 0 {
            return Ok(Some(value));
        }

        shift += 7;
        if shift >= 64 {
            return Err(anyhow::anyhow!("varint too large"));
        }
    }
}

async fn read_u8<R: AsyncRead + Unpin>(reader: &mut R) -> Result<u8> {
    let mut buf = [0u8; 1];
    tokio::io::AsyncReadExt::read_exact(reader, &mut buf)
        .await
        .context("unexpected EOF while reading byte")?;
    Ok(buf[0])
}

fn resolve_checkpoint_range(args: &Args, blob: &BlobMetadata) -> Result<(u64, u64)> {
    let mut range_start = args.start_checkpoint.unwrap_or(blob.start_checkpoint);
    let mut range_end = match args.end_checkpoint {
        Some(end_inclusive) => end_inclusive.saturating_add(1),
        None => blob.end_checkpoint.saturating_add(1),
    };

    if args.from_latest {
        range_end = blob.end_checkpoint.saturating_add(1);
        if args.limit > 0 {
            range_start = range_end.saturating_sub(args.limit);
        }
    } else if args.limit > 0 {
        if args.start_checkpoint.is_some() && args.end_checkpoint.is_none() {
            range_end = range_start.saturating_add(args.limit);
        } else if args.start_checkpoint.is_none() && args.end_checkpoint.is_none() {
            range_end = blob.end_checkpoint.saturating_add(1);
            range_start = range_end.saturating_sub(args.limit);
        }
    }

    range_start = range_start.max(blob.start_checkpoint);
    range_end = range_end.min(blob.end_checkpoint.saturating_add(1));

    if range_end <= range_start {
        return Err(anyhow::anyhow!(
            "invalid checkpoint range {}..{} for blob coverage {}..{}",
            range_start,
            range_end,
            blob.start_checkpoint,
            blob.end_checkpoint
        ));
    }

    Ok((range_start, range_end))
}

fn filter_entries(
    index: &HashMap<u64, BlobIndexEntry>,
    start: u64,
    end: u64,
) -> Vec<(u64, BlobIndexEntry)> {
    index
        .iter()
        .filter_map(|(cp, entry)| {
            if *cp >= start && *cp < end {
                Some((*cp, entry.clone()))
            } else {
                None
            }
        })
        .collect()
}

fn coalesce_entries(
    entries: &[(u64, BlobIndexEntry)],
    max_gap_bytes: u64,
    max_range_bytes: u64,
) -> Vec<RangePlan> {
    if entries.is_empty() {
        return Vec::new();
    }

    let max_range_bytes = if max_range_bytes == 0 {
        u64::MAX
    } else {
        max_range_bytes
    };

    let mut ranges = Vec::new();
    let mut current = RangePlan {
        start: entries[0].1.offset,
        end: entries[0].1.offset + entries[0].1.length,
        entries: vec![entries[0].clone()],
    };

    let mut last_end = current.end;

    for (checkpoint, entry) in entries.iter().skip(1) {
        let entry_start = entry.offset;
        let entry_end = entry.offset + entry.length;
        let gap = entry_start.saturating_sub(last_end);
        let combined_len = entry_end.saturating_sub(current.start);

        if gap <= max_gap_bytes && combined_len <= max_range_bytes {
            current.end = current.end.max(entry_end);
            current.entries.push((*checkpoint, entry.clone()));
            last_end = entry_end;
        } else {
            ranges.push(current);
            current = RangePlan {
                start: entry_start,
                end: entry_end,
                entries: vec![(*checkpoint, entry.clone())],
            };
            last_end = entry_end;
        }
    }

    ranges.push(current);
    ranges
}

fn chunk_entries(
    entries: &[(u64, BlobIndexEntry)],
    total_size: u64,
    chunk_count: u64,
    merge_overlaps: bool,
) -> Vec<RangePlan> {
    if entries.is_empty() || chunk_count == 0 {
        return Vec::new();
    }

    let chunk_count = chunk_count.max(1);
    let chunk_size = (total_size + chunk_count - 1) / chunk_count;
    if chunk_size == 0 {
        return Vec::new();
    }

    let mut chunks = Vec::with_capacity(chunk_count as usize);
    for idx in 0..chunk_count {
        let start = idx * chunk_size;
        let end = ((idx + 1) * chunk_size).min(total_size);
        chunks.push(RangePlan {
            start,
            end,
            entries: Vec::new(),
        });
    }

    for (checkpoint, entry) in entries {
        let mut idx = (entry.offset / chunk_size) as usize;
        if idx >= chunks.len() {
            idx = chunks.len() - 1;
        }
        chunks[idx].entries.push((*checkpoint, entry.clone()));
    }

    let mut merged: Vec<RangePlan> = Vec::new();

    for mut chunk in chunks.into_iter() {
        if chunk.entries.is_empty() {
            continue;
        }

        chunk.entries.sort_by_key(|(_, entry)| entry.offset);
        if let Some(max_end) = chunk
            .entries
            .iter()
            .map(|(_, entry)| entry.offset + entry.length)
            .max()
        {
            if max_end > chunk.end {
                chunk.end = max_end;
            }
        }

        if merge_overlaps {
            if let Some(prev) = merged.last_mut() {
                if chunk.start <= prev.end {
                    prev.end = prev.end.max(chunk.end);
                    prev.entries.extend(chunk.entries);
                    prev.entries.sort_by_key(|(_, entry)| entry.offset);
                    continue;
                }
            }
        }

        merged.push(chunk);
    }

    merged
}

async fn process_ranges_via_http(
    client: &Client,
    aggregator_url: &str,
    blob_id: &str,
    ranges: &[RangeWork],
    resume: Option<Arc<ResumeHandle>>,
    package_id: Option<ObjectID>,
    stats: &mut Stats,
) -> Result<()> {
    for work in ranges {
        let range = &work.range;
        if range.len() == 0 {
            continue;
        }

        let bytes = download_range_http(client, aggregator_url, blob_id, range.start, range.len())
            .await
            .with_context(|| format!("failed to download range {}-{}", range.start, range.end))?;

        stats.ranges += 1;
        stats.bytes += range.len();

        if package_id.is_some() {
            process_range_bytes(range, &bytes, package_id, stats)?;
        } else {
            process_range_bytes(range, &bytes, None, stats)?;
        }

        if let Some(resume) = &resume {
            resume.mark_completed(blob_id, work.idx).await?;
        }
    }

    Ok(())
}

async fn process_ranges_via_cli(
    cli_path: &Path,
    args: &Args,
    blob_id: &str,
    ranges: &[RangeWork],
    resume: Option<Arc<ResumeHandle>>,
    package_id: Option<ObjectID>,
    stats: &mut Stats,
) -> Result<()> {
    if args.stream_ranges {
        return process_ranges_via_cli_streaming(
            cli_path,
            args,
            blob_id,
            ranges,
            resume,
            package_id,
            stats,
        )
        .await;
    }

    let ranges = ranges
        .iter()
        .cloned()
        .filter(|work| work.range.len() > 0);
    let concurrency = args.range_concurrency.max(1);

    if concurrency == 1 {
        for work in ranges {
            let range = &work.range;
            let buffer =
                read_range_via_cli(cli_path, args, blob_id, range.start, range.len()).await?;

            if buffer.len() as u64 != range.len() {
                eprintln!(
                    "warning: expected {} bytes but received {} for range {}-{}",
                    range.len(),
                    buffer.len(),
                    range.start,
                    range.end
                );
            }

            stats.ranges += 1;
            stats.bytes += buffer.len() as u64;

            process_range_bytes(range, &buffer, package_id, stats)?;

            if let Some(resume) = &resume {
                resume.mark_completed(blob_id, work.idx).await?;
            }
        }
        return Ok(());
    }

    let cli_path = cli_path.to_path_buf();
    let args = args.clone();
    let resume = resume.clone();
    let mut stream = stream::iter(ranges)
        .map(|work| {
            let cli_path = cli_path.clone();
            let args = args.clone();
            let package_id = package_id.clone();
            let resume = resume.clone();
            async move {
                let range = &work.range;
                let buffer =
                    read_range_via_cli(&cli_path, &args, blob_id, range.start, range.len()).await?;

                if buffer.len() as u64 != range.len() {
                    eprintln!(
                        "warning: expected {} bytes but received {} for range {}-{}",
                        range.len(),
                        buffer.len(),
                        range.start,
                        range.end
                    );
                }

                let mut local = Stats::new();
                local.ranges = 1;
                local.bytes = buffer.len() as u64;
                process_range_bytes(range, &buffer, package_id, &mut local)?;

                if let Some(resume) = resume {
                    resume.mark_completed(blob_id, work.idx).await?;
                }
                Ok::<Stats, anyhow::Error>(local)
            }
        })
        .buffer_unordered(concurrency);

    while let Some(result) = stream.next().await {
        let local = result?;
        stats.merge(&local);
    }

    Ok(())
}

async fn process_ranges_via_cli_streaming(
    cli_path: &Path,
    args: &Args,
    blob_id: &str,
    ranges: &[RangeWork],
    resume: Option<Arc<ResumeHandle>>,
    package_id: Option<ObjectID>,
    stats: &mut Stats,
) -> Result<()> {
    let ranges = ranges
        .iter()
        .cloned()
        .filter(|work| work.range.len() > 0);
    let concurrency = args.range_concurrency.max(1);

    if concurrency == 1 {
        for work in ranges {
            let local = process_single_range_streaming(
                cli_path,
                args,
                blob_id,
                &work.range,
                package_id,
            )
            .await?;
            stats.merge(&local);
            if let Some(resume) = &resume {
                resume.mark_completed(blob_id, work.idx).await?;
            }
        }
        return Ok(());
    }

    let cli_path = cli_path.to_path_buf();
    let args = args.clone();
    let resume = resume.clone();
    let mut stream = stream::iter(ranges)
        .map(|work| {
            let cli_path = cli_path.clone();
            let args = args.clone();
            let package_id = package_id.clone();
            let resume = resume.clone();
            async move {
                let local = process_single_range_streaming(
                    &cli_path,
                    &args,
                    blob_id,
                    &work.range,
                    package_id,
                )
                .await?;
                if let Some(resume) = resume {
                    resume.mark_completed(blob_id, work.idx).await?;
                }
                Ok::<Stats, anyhow::Error>(local)
            }
        })
        .buffer_unordered(concurrency);

    while let Some(result) = stream.next().await {
        let local = result?;
        stats.merge(&local);
    }

    Ok(())
}

async fn process_single_range_streaming(
    cli_path: &Path,
    args: &Args,
    blob_id: &str,
    range: &RangePlan,
    package_id: Option<ObjectID>,
) -> Result<Stats> {
    with_retry(|| async {
        process_single_range_streaming_once(cli_path, args, blob_id, range, package_id).await
    })
    .await
}

async fn process_single_range_streaming_once(
    cli_path: &Path,
    args: &Args,
    blob_id: &str,
    range: &RangePlan,
    package_id: Option<ObjectID>,
) -> Result<Stats> {
    let mut cmd = walrus_read_command(cli_path, args);
    cmd.arg(blob_id)
        .arg("--start-byte")
        .arg(range.start.to_string())
        .arg("--byte-length")
        .arg(range.len().to_string());

    let mut child = cmd
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::inherit())
        .spawn()
        .context("failed to spawn walrus cli")?;

    let mut stdout = child
        .stdout
        .take()
        .context("failed to capture walrus cli stdout")?;

    let mut stats = Stats::new();
    stats.ranges = 1;
    stats.bytes = range.len();

    let mut cursor = range.start;

    for (checkpoint, entry) in &range.entries {
        if entry.offset < cursor {
            let _ = child.kill().await;
            let _ = child.wait().await;
            return Err(anyhow::anyhow!(
                "range stream out of order for checkpoint {} (offset {} < {})",
                checkpoint,
                entry.offset,
                cursor
            ));
        }

        let skip = entry.offset - cursor;
        if skip > 0 {
            if let Err(err) = drain_exact(&mut stdout, skip).await {
                let _ = child.kill().await;
                let _ = child.wait().await;
                return Err(err);
            }
        }

        let len = usize::try_from(entry.length)
            .with_context(|| format!("entry length too large for checkpoint {}", checkpoint))?;
        let mut data = vec![0u8; len];
        if let Err(err) = tokio::io::AsyncReadExt::read_exact(&mut stdout, &mut data).await {
            let _ = child.kill().await;
            let _ = child.wait().await;
            return Err(err).with_context(|| {
                format!("unexpected EOF while reading checkpoint {}", checkpoint)
            });
        }
        cursor = entry.offset.saturating_add(entry.length);

        let checkpoint_data = sui_storage::blob::Blob::from_bytes::<CheckpointData>(&data)
            .with_context(|| format!("failed to deserialize checkpoint {}", checkpoint))?;

        stats.checkpoints += 1;
        if let Some(package_id) = package_id {
            let (events, objects) = count_package_hits(&checkpoint_data, package_id);
            stats.events += events;
            stats.objects += objects;
        }
    }

    let status = child.wait().await?;
    if !status.success() {
        return Err(anyhow::anyhow!("walrus cli exited with status {}", status));
    }

    Ok(stats)
}

fn process_range_bytes(
    range: &RangePlan,
    bytes: &[u8],
    package_id: Option<ObjectID>,
    stats: &mut Stats,
) -> Result<()> {
    for (checkpoint, entry) in &range.entries {
        let start = entry.offset.saturating_sub(range.start) as usize;
        let end = start + entry.length as usize;
        if end > bytes.len() {
            return Err(anyhow::anyhow!(
                "range buffer too small for checkpoint {} (needed {}, have {})",
                checkpoint,
                end,
                bytes.len()
            ));
        }

        let checkpoint_data = sui_storage::blob::Blob::from_bytes::<CheckpointData>(
            &bytes[start..end],
        )
        .with_context(|| format!("failed to deserialize checkpoint {}", checkpoint))?;

        stats.checkpoints += 1;

        if let Some(package_id) = package_id {
            let (events, objects) = count_package_hits(&checkpoint_data, package_id);
            stats.events += events;
            stats.objects += objects;
        }
    }

    Ok(())
}

fn count_package_hits(checkpoint: &CheckpointData, package_id: ObjectID) -> (u64, u64) {
    let mut event_count = 0u64;
    let mut object_count = 0u64;

    for tx in &checkpoint.transactions {
        if let Some(events) = &tx.events {
            for event in &events.data {
                if event.package_id == package_id {
                    event_count += 1;
                }
            }
        }

        for object in &tx.output_objects {
            let Some(type_) = object.type_() else {
                continue;
            };

            if type_.address() == package_id.into() {
                object_count += 1;
            }
        }
    }

    (event_count, object_count)
}

async fn download_range_http(
    client: &Client,
    aggregator_url: &str,
    blob_id: &str,
    start: u64,
    length: u64,
) -> Result<Vec<u8>> {
    let end = start + length - 1;
    let url = format!("{}/v1/blobs/{}", aggregator_url, blob_id);

    let response = with_retry(|| {
        let client = client.clone();
        let url = url.clone();
        async move {
            let response = client
                .get(&url)
                .header("Range", format!("bytes={}-{}", start, end))
                .send()
                .await?;

            if !response.status().is_success() {
                return Err(anyhow::anyhow!("status {}", response.status()));
            }

            Ok(response)
        }
    })
    .await?;

    let mut stream = response.bytes_stream();
    let mut buf = Vec::with_capacity(length as usize);

    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        buf.extend_from_slice(&chunk);
    }

    if buf.len() as u64 != length {
        eprintln!(
            "warning: expected {} bytes but received {}",
            length,
            buf.len()
        );
    }

    Ok(buf)
}

async fn run_raw_download(client: &Client, args: &Args, blob: &BlobMetadata) -> Result<Stats> {
    let mut stats = Stats::new();

    match args.transport {
        Transport::Http => {
            let mut ranges = if args.chunk_count > 0 {
                build_raw_ranges(blob.total_size, args.chunk_count)
            } else {
                vec![RangePlan {
                    start: 0,
                    end: blob.total_size,
                    entries: Vec::new(),
                }]
            };

            if let Some(chunk_index) = args.chunk_index {
                ranges = select_chunk_range(ranges, chunk_index)?;
            }

            for range in ranges {
                let bytes = download_range_http(
                    client,
                    &args.aggregator_url,
                    &blob.blob_id,
                    range.start,
                    range.len(),
                )
                .await?;
                stats.ranges += 1;
                stats.bytes += bytes.len() as u64;
            }
        }
        Transport::Cli => {
            let cli_path = args
                .walrus_cli_path
                .as_ref()
                .expect("cli path checked above");

            if args.chunk_count > 0 && args.chunk_index.is_some() {
                let ranges = build_raw_ranges(blob.total_size, args.chunk_count);
                let range = select_chunk_range(ranges, args.chunk_index.unwrap())?
                    .pop()
                    .expect("chunk selection");
                let len = drain_cli_read_range(cli_path, args, &blob.blob_id, range.start, range.len()).await?;

                stats.ranges = 1;
                stats.bytes = len;
            } else {
                if args.chunk_count > 0 {
                    eprintln!(
                        "chunking is not supported for CLI raw download without --chunk-index; streaming full blob"
                    );
                }

                let bytes = drain_cli_read_range(cli_path, args, &blob.blob_id, 0, 0).await?;
                stats.ranges = 1;
                stats.bytes = bytes;
            }
        }
    }

    Ok(stats)
}

fn build_raw_ranges(total_size: u64, chunk_count: u64) -> Vec<RangePlan> {
    if chunk_count == 0 {
        return Vec::new();
    }

    let chunk_size = (total_size + chunk_count - 1) / chunk_count;
    let mut ranges = Vec::new();

    for idx in 0..chunk_count {
        let start = idx * chunk_size;
        let end = ((idx + 1) * chunk_size).min(total_size);
        if start >= end {
            continue;
        }
        ranges.push(RangePlan {
            start,
            end,
            entries: Vec::new(),
        });
    }

    ranges
}

fn build_single_window(entries: &[(u64, BlobIndexEntry)]) -> Result<Vec<RangePlan>> {
    if entries.is_empty() {
        return Ok(Vec::new());
    }

    let first = entries
        .iter()
        .min_by_key(|(_, entry)| entry.offset)
        .expect("non-empty");
    let last = entries
        .iter()
        .max_by_key(|(_, entry)| entry.offset.saturating_add(entry.length))
        .expect("non-empty");

    let start = first.1.offset;
    let end = last.1.offset.saturating_add(last.1.length);

    Ok(vec![RangePlan {
        start,
        end,
        entries: entries.to_vec(),
    }])
}

async fn drain_exact<R: AsyncRead + Unpin>(reader: &mut R, mut bytes: u64) -> Result<()> {
    let mut buf = vec![0u8; 1024 * 1024];
    while bytes > 0 {
        let to_read = std::cmp::min(bytes as usize, buf.len());
        let n = tokio::io::AsyncReadExt::read(reader, &mut buf[..to_read]).await?;
        if n == 0 {
            return Err(anyhow::anyhow!("unexpected EOF while draining"));
        }
        bytes -= n as u64;
    }
    Ok(())
}

fn estimate_index_buffer_bytes(entries_count: u64) -> u64 {
    // name_len (u32) + name bytes (max 20 digits) + offset(u64) + length(u64) + crc(u32)
    let max_per_entry = 4u64 + 20u64 + 8u64 + 8u64 + 4u64;
    entries_count.saturating_mul(max_per_entry).saturating_add(24)
}

async fn extract_index_via_cli(
    cli_path: &Path,
    args: &Args,
    blob: &BlobMetadata,
    override_bytes: u64,
) -> Result<HashMap<u64, BlobIndexEntry>> {
    let max_tail = if override_bytes > 0 {
        override_bytes
    } else {
        estimate_index_buffer_bytes(blob.entries_count)
    };

    eprintln!(
        "index pass via CLI for blob {} (tail buffer {} bytes)",
        blob.blob_id, max_tail
    );

    let (tail, read_len) =
        read_tail_via_cli(cli_path, args, &blob.blob_id, blob.total_size, max_tail as usize)
            .await?;

    if tail.len() < 24 {
        return Err(anyhow::anyhow!("tail buffer too small to read footer"));
    }

    let total_size = if read_len > 0 { read_len } else { blob.total_size };
    if total_size != blob.total_size {
        eprintln!(
            "warning: blob size mismatch (metadata {}, read {})",
            blob.total_size, total_size
        );
    }

    let footer = &tail[tail.len() - 24..];
    let mut cursor = Cursor::new(footer);
    let magic = cursor.read_u32::<LittleEndian>()?;
    if magic != 0x574c4244 {
        return Err(anyhow::anyhow!("invalid footer magic: {:x}", magic));
    }
    let _version = cursor.read_u32::<LittleEndian>()?;
    let index_start_offset = cursor.read_u64::<LittleEndian>()?;
    let count = cursor.read_u32::<LittleEndian>()?;

    let index_size = total_size
        .checked_sub(index_start_offset)
        .ok_or_else(|| anyhow::anyhow!("invalid index offset"))?;

    if index_size as usize > tail.len() {
        return Err(anyhow::anyhow!(
            "index bytes ({}) exceed tail buffer ({}). Increase --index-buffer-bytes.",
            index_size,
            tail.len()
        ));
    }

    let index_start = tail.len() - index_size as usize;
    let index_bytes = &tail[index_start..];

    parse_index_bytes(index_bytes, count)
}

async fn read_tail_via_cli(
    cli_path: &Path,
    args: &Args,
    blob_id: &str,
    total_size: u64,
    max_tail: usize,
) -> Result<(Vec<u8>, u64)> {
    if total_size == 0 {
        return Err(anyhow::anyhow!("blob size unknown; cannot read tail"));
    }

    let read_len = max_tail.min(total_size as usize) as u64;
    let start = total_size.saturating_sub(read_len);
    let (tail, reported_size) =
        read_range_via_cli_capture(cli_path, args, blob_id, start, read_len).await?;

    if let Some(size) = reported_size {
        if size != total_size {
            let adjusted_len = max_tail.min(size as usize) as u64;
            let adjusted_start = size.saturating_sub(adjusted_len);
            if adjusted_start != start {
                let (tail, _) =
                    read_range_via_cli_capture(cli_path, args, blob_id, adjusted_start, adjusted_len)
                        .await?;
                return Ok((tail, size));
            }
            return Ok((tail, size));
        }
    }

    Ok((tail, 0))
}

async fn load_index_from_file(path: &Path) -> Result<HashMap<u64, BlobIndexEntry>> {
    let data = tokio::fs::read(path)
        .await
        .with_context(|| format!("failed to read index file {}", path.display()))?;
    let index = serde_json::from_slice(&data)?;
    Ok(index)
}

async fn drain_reader<R: AsyncRead + Unpin>(reader: &mut R) -> Result<u64> {
    let mut buf = vec![0u8; 1024 * 1024];
    let mut total = 0u64;

    loop {
        let n = tokio::io::AsyncReadExt::read(reader, &mut buf).await?;
        if n == 0 {
            break;
        }
        total += n as u64;
    }

    Ok(total)
}

async fn read_range_via_cli(
    cli_path: &Path,
    args: &Args,
    blob_id: &str,
    start: u64,
    length: u64,
) -> Result<Vec<u8>> {
    with_retry(|| async {
        let mut cmd = walrus_read_command(cli_path, args);
        cmd.arg(blob_id);
        if length > 0 {
            cmd.arg("--start-byte").arg(start.to_string());
            cmd.arg("--byte-length").arg(length.to_string());
        }

        let mut child = cmd
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::inherit())
            .spawn()
            .context("failed to spawn walrus cli")?;

        let mut stdout = child
            .stdout
            .take()
            .context("failed to capture walrus cli stdout")?;

        let mut buffer = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut stdout, &mut buffer).await?;

        let status = child.wait().await?;
        if !status.success() {
            return Err(anyhow::anyhow!("walrus cli exited with status {}", status));
        }

        Ok(buffer)
    })
    .await
}

async fn read_range_via_cli_capture(
    cli_path: &Path,
    args: &Args,
    blob_id: &str,
    start: u64,
    length: u64,
) -> Result<(Vec<u8>, Option<u64>)> {
    with_retry(|| async {
        read_range_via_cli_capture_once(cli_path, args, blob_id, start, length).await
    })
    .await
}

async fn read_range_via_cli_capture_once(
    cli_path: &Path,
    args: &Args,
    blob_id: &str,
    start: u64,
    length: u64,
) -> Result<(Vec<u8>, Option<u64>)> {
    let mut cmd = walrus_read_command(cli_path, args);
    cmd.arg(blob_id);
    cmd.arg("--start-byte").arg(start.to_string());
    cmd.arg("--byte-length").arg(length.to_string());

    let mut child = cmd
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .context("failed to spawn walrus cli")?;

    let mut stdout = child
        .stdout
        .take()
        .context("failed to capture walrus cli stdout")?;
    let mut stderr = child
        .stderr
        .take()
        .context("failed to capture walrus cli stderr")?;

    let mut out_buf = Vec::new();
    let mut err_buf = Vec::new();
    tokio::try_join!(
        tokio::io::AsyncReadExt::read_to_end(&mut stdout, &mut out_buf),
        tokio::io::AsyncReadExt::read_to_end(&mut stderr, &mut err_buf),
    )?;

    let status = child.wait().await?;
    let stderr_str = String::from_utf8_lossy(&err_buf);
    if !stderr_str.is_empty() {
        eprint!("{}", stderr_str);
    }

    if !status.success() {
        let completed = stderr_str.contains("finished reading byte range")
            && out_buf.len() as u64 == length;
        if completed {
            eprintln!(
                "warning: walrus cli exited with status {}; using range output",
                status
            );
        } else {
            return Err(anyhow::anyhow!("walrus cli exited with status {}", status));
        }
    }
    let size = parse_blob_size_from_stderr(&stderr_str);

    Ok((out_buf, size))
}

fn parse_blob_size_from_stderr(stderr: &str) -> Option<u64> {
    let cleaned = strip_ansi(stderr);
    let marker = "blob_size=";
    let idx = cleaned.rfind(marker)?;
    let rest = &cleaned[idx + marker.len()..];
    let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
    if digits.is_empty() {
        None
    } else {
        digits.parse().ok()
    }
}

fn strip_ansi(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch == '\u{1b}' {
            if matches!(chars.peek(), Some('[')) {
                let _ = chars.next();
                while let Some(c) = chars.next() {
                    if c == 'm' {
                        break;
                    }
                }
            }
            continue;
        }
        out.push(ch);
    }

    out
}

async fn drain_cli_read_range(
    cli_path: &Path,
    args: &Args,
    blob_id: &str,
    start: u64,
    length: u64,
) -> Result<u64> {
    let mut cmd = walrus_read_command(cli_path, args);
    cmd.arg(blob_id);
    if length > 0 {
        cmd.arg("--start-byte").arg(start.to_string());
        cmd.arg("--byte-length").arg(length.to_string());
    }

    let mut child = cmd
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::inherit())
        .spawn()
        .context("failed to spawn walrus cli")?;

    let mut stdout = child
        .stdout
        .take()
        .context("failed to capture walrus cli stdout")?;

    let bytes = drain_reader(&mut stdout).await?;
    let status = child.wait().await?;
    if !status.success() {
        return Err(anyhow::anyhow!("walrus cli exited with status {}", status));
    }

    Ok(bytes)
}

async fn fetch_blobs(client: &Client, archival_url: &str) -> Result<Vec<BlobMetadata>> {
    let url = format!("{}/v1/app_blobs", archival_url);
    let response = client
        .get(&url)
        .send()
        .await
        .with_context(|| format!("failed to fetch blobs from {}", url))?;

    if !response.status().is_success() {
        return Err(anyhow::anyhow!(
            "archival service returned status {}",
            response.status()
        ));
    }

    let response: BlobsResponse = response.json().await?;
    Ok(response.blobs)
}

async fn fetch_index(
    client: &Client,
    aggregator_url: &str,
    blob_id: &str,
) -> Result<HashMap<u64, BlobIndexEntry>> {
    let url = format!("{}/v1/blobs/{}", aggregator_url, blob_id);

    let footer_bytes = with_retry(|| {
        let client = client.clone();
        let url = url.clone();
        async move {
            let response = client
                .get(&url)
                .header("Range", "bytes=-24")
                .send()
                .await?;

            if !response.status().is_success() {
                return Err(anyhow::anyhow!("status {}", response.status()));
            }

            Ok(response.bytes().await?)
        }
    })
    .await
    .with_context(|| format!("failed to fetch footer for blob {}", blob_id))?;

    if footer_bytes.len() != 24 {
        return Err(anyhow::anyhow!(
            "invalid footer length: {}",
            footer_bytes.len()
        ));
    }

    let mut cursor = Cursor::new(&footer_bytes);
    let magic = cursor.read_u32::<LittleEndian>()?;
    if magic != 0x574c4244 {
        return Err(anyhow::anyhow!("invalid footer magic: {:x}", magic));
    }

    let _version = cursor.read_u32::<LittleEndian>()?;
    let index_start_offset = cursor.read_u64::<LittleEndian>()?;
    let count = cursor.read_u32::<LittleEndian>()?;

    let index_bytes = with_retry(|| {
        let client = client.clone();
        let url = url.clone();
        async move {
            let response = client
                .get(&url)
                .header("Range", format!("bytes={}-", index_start_offset))
                .send()
                .await?;

            if !response.status().is_success() {
                return Err(anyhow::anyhow!("status {}", response.status()));
            }

            Ok(response.bytes().await?)
        }
    })
    .await
    .with_context(|| format!("failed to fetch index for blob {}", blob_id))?;

    parse_index_bytes(&index_bytes, count)
}

fn parse_index_bytes(index_bytes: &[u8], count: u32) -> Result<HashMap<u64, BlobIndexEntry>> {
    let mut cursor = Cursor::new(index_bytes);
    let mut index = HashMap::with_capacity(count as usize);

    for _ in 0..count {
        let name_len = cursor.read_u32::<LittleEndian>()?;
        let mut name_bytes = vec![0u8; name_len as usize];
        cursor.read_exact(&mut name_bytes)?;
        let name_str = String::from_utf8(name_bytes).context("invalid utf8 in checkpoint name")?;
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

    Ok(index)
}

async fn with_retry<F, Fut, T>(mut f: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let mut attempts = 0;
    let max_attempts = 5;

    loop {
        match f().await {
            Ok(res) => return Ok(res),
            Err(err) if attempts < max_attempts => {
                attempts += 1;
                eprintln!("request failed (attempt {}): {}", attempts, err);
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
            Err(err) => return Err(err),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use byteorder::WriteBytesExt;
    use std::io::Write;

    fn create_test_index_bytes(entries: &[(u64, u64, u64)]) -> Vec<u8> {
        let mut buf = Vec::new();
        for (checkpoint, offset, length) in entries {
            let name = checkpoint.to_string();
            buf.write_u32::<LittleEndian>(name.len() as u32).unwrap();
            buf.write_all(name.as_bytes()).unwrap();
            buf.write_u64::<LittleEndian>(*offset).unwrap();
            buf.write_u64::<LittleEndian>(*length).unwrap();
            buf.write_u32::<LittleEndian>(0).unwrap(); // CRC placeholder
        }
        buf
    }

    #[test]
    fn test_parse_index_bytes() {
        let entries = vec![
            (1000u64, 0u64, 1024u64),
            (1001u64, 1024u64, 2048u64),
            (1002u64, 3072u64, 512u64),
        ];
        let index_bytes = create_test_index_bytes(&entries);
        let index = parse_index_bytes(&index_bytes, 3).unwrap();

        assert_eq!(index.len(), 3);
        assert_eq!(index.get(&1000).unwrap().offset, 0);
        assert_eq!(index.get(&1000).unwrap().length, 1024);
        assert_eq!(index.get(&1001).unwrap().offset, 1024);
        assert_eq!(index.get(&1002).unwrap().length, 512);
    }

    #[test]
    fn test_parse_index_bytes_empty() {
        let index = parse_index_bytes(&[], 0).unwrap();
        assert!(index.is_empty());
    }

    #[test]
    fn test_coalesce_entries_empty() {
        let entries: Vec<(u64, BlobIndexEntry)> = vec![];
        let ranges = coalesce_entries(&entries, 0, u64::MAX);
        assert!(ranges.is_empty());
    }

    #[test]
    fn test_coalesce_entries_single() {
        let entries = vec![(
            1000u64,
            BlobIndexEntry {
                checkpoint_number: 1000,
                offset: 0,
                length: 1024,
            },
        )];
        let ranges = coalesce_entries(&entries, 0, u64::MAX);
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].start, 0);
        assert_eq!(ranges[0].end, 1024);
        assert_eq!(ranges[0].entries.len(), 1);
    }

    #[test]
    fn test_coalesce_entries_contiguous() {
        let entries = vec![
            (
                1000u64,
                BlobIndexEntry {
                    checkpoint_number: 1000,
                    offset: 0,
                    length: 1024,
                },
            ),
            (
                1001u64,
                BlobIndexEntry {
                    checkpoint_number: 1001,
                    offset: 1024,
                    length: 1024,
                },
            ),
        ];
        let ranges = coalesce_entries(&entries, 0, u64::MAX);
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].start, 0);
        assert_eq!(ranges[0].end, 2048);
        assert_eq!(ranges[0].entries.len(), 2);
    }

    #[test]
    fn test_coalesce_entries_with_gap() {
        let entries = vec![
            (
                1000u64,
                BlobIndexEntry {
                    checkpoint_number: 1000,
                    offset: 0,
                    length: 1024,
                },
            ),
            (
                1001u64,
                BlobIndexEntry {
                    checkpoint_number: 1001,
                    offset: 2048, // Gap of 1024 bytes
                    length: 1024,
                },
            ),
        ];

        // With gap tolerance of 0, should create 2 ranges
        let ranges = coalesce_entries(&entries, 0, u64::MAX);
        assert_eq!(ranges.len(), 2);

        // With gap tolerance of 1024, should coalesce into 1 range
        let ranges = coalesce_entries(&entries, 1024, u64::MAX);
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].start, 0);
        assert_eq!(ranges[0].end, 3072);
    }

    #[test]
    fn test_coalesce_entries_max_range_size() {
        let entries = vec![
            (
                1000u64,
                BlobIndexEntry {
                    checkpoint_number: 1000,
                    offset: 0,
                    length: 1024,
                },
            ),
            (
                1001u64,
                BlobIndexEntry {
                    checkpoint_number: 1001,
                    offset: 1024,
                    length: 1024,
                },
            ),
            (
                1002u64,
                BlobIndexEntry {
                    checkpoint_number: 1002,
                    offset: 2048,
                    length: 1024,
                },
            ),
        ];

        // With max range size of 2048, should create 2 ranges
        let ranges = coalesce_entries(&entries, 0, 2048);
        assert_eq!(ranges.len(), 2);
        assert_eq!(ranges[0].entries.len(), 2);
        assert_eq!(ranges[1].entries.len(), 1);
    }

    #[test]
    fn test_filter_entries() {
        let mut index = HashMap::new();
        for i in 1000..1010 {
            index.insert(
                i,
                BlobIndexEntry {
                    checkpoint_number: i,
                    offset: (i - 1000) * 1024,
                    length: 1024,
                },
            );
        }

        let filtered = filter_entries(&index, 1003, 1007);
        assert_eq!(filtered.len(), 4); // 1003, 1004, 1005, 1006
    }

    #[test]
    fn test_chunk_entries() {
        let entries: Vec<(u64, BlobIndexEntry)> = (0..100)
            .map(|i| {
                (
                    1000 + i,
                    BlobIndexEntry {
                        checkpoint_number: 1000 + i,
                        offset: i * 1000,
                        length: 1000,
                    },
                )
            })
            .collect();

        let total_size = 100_000;
        let ranges = chunk_entries(&entries, total_size, 4, false);

        assert_eq!(ranges.len(), 4);
        // Each chunk should have roughly 25 entries
        for range in &ranges {
            assert!(range.entries.len() >= 20 && range.entries.len() <= 30);
        }
    }

    #[test]
    fn test_build_raw_ranges() {
        let ranges = build_raw_ranges(1000, 4);
        assert_eq!(ranges.len(), 4);
        assert_eq!(ranges[0].start, 0);
        assert_eq!(ranges[0].end, 250);
        assert_eq!(ranges[3].start, 750);
        assert_eq!(ranges[3].end, 1000);
    }

    #[test]
    fn test_strip_ansi() {
        let input = "\x1b[32mgreen text\x1b[0m normal";
        let output = strip_ansi(input);
        assert_eq!(output, "green text normal");
    }

    #[test]
    fn test_strip_ansi_no_codes() {
        let input = "plain text";
        let output = strip_ansi(input);
        assert_eq!(output, "plain text");
    }

    #[test]
    fn test_parse_blob_size_from_stderr() {
        let stderr = "some output blob_size=12345 more output";
        let size = parse_blob_size_from_stderr(stderr);
        assert_eq!(size, Some(12345));
    }

    #[test]
    fn test_parse_blob_size_from_stderr_with_ansi() {
        let stderr = "\x1b[32mblob_size=\x1b[0m99999";
        let size = parse_blob_size_from_stderr(stderr);
        assert_eq!(size, Some(99999));
    }

    #[test]
    fn test_parse_blob_size_from_stderr_not_found() {
        let stderr = "no size here";
        let size = parse_blob_size_from_stderr(stderr);
        assert_eq!(size, None);
    }

    #[test]
    fn test_select_blobs_by_id() {
        let blobs = vec![
            BlobMetadata {
                blob_id: "abc".to_string(),
                start_checkpoint: 0,
                end_checkpoint: 100,
                entries_count: 100,
                total_size: 1000,
                end_of_epoch: false,
                expiry_epoch: 0,
            },
            BlobMetadata {
                blob_id: "def".to_string(),
                start_checkpoint: 101,
                end_checkpoint: 200,
                entries_count: 100,
                total_size: 1000,
                end_of_epoch: false,
                expiry_epoch: 0,
            },
        ];

        let selected = select_blobs(&blobs, Some("def"), 1).unwrap();
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].blob_id, "def");
    }

    #[test]
    fn test_select_blobs_by_count() {
        let blobs = vec![
            BlobMetadata {
                blob_id: "oldest".to_string(),
                start_checkpoint: 0,
                end_checkpoint: 100,
                entries_count: 100,
                total_size: 1000,
                end_of_epoch: false,
                expiry_epoch: 0,
            },
            BlobMetadata {
                blob_id: "middle".to_string(),
                start_checkpoint: 101,
                end_checkpoint: 200,
                entries_count: 100,
                total_size: 1000,
                end_of_epoch: false,
                expiry_epoch: 0,
            },
            BlobMetadata {
                blob_id: "newest".to_string(),
                start_checkpoint: 201,
                end_checkpoint: 300,
                entries_count: 100,
                total_size: 1000,
                end_of_epoch: false,
                expiry_epoch: 0,
            },
        ];

        let selected = select_blobs(&blobs, None, 2).unwrap();
        assert_eq!(selected.len(), 2);
        // Should be sorted by end_checkpoint descending
        assert_eq!(selected[0].blob_id, "newest");
        assert_eq!(selected[1].blob_id, "middle");
    }

    #[test]
    fn test_build_single_window() {
        let entries = vec![
            (
                1000u64,
                BlobIndexEntry {
                    checkpoint_number: 1000,
                    offset: 100,
                    length: 50,
                },
            ),
            (
                1001u64,
                BlobIndexEntry {
                    checkpoint_number: 1001,
                    offset: 500,
                    length: 100,
                },
            ),
        ];

        let ranges = build_single_window(&entries).unwrap();
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].start, 100);
        assert_eq!(ranges[0].end, 600);
        assert_eq!(ranges[0].entries.len(), 2);
    }

    #[test]
    fn test_estimate_index_buffer_bytes() {
        // Should return reasonable buffer size for index
        let size = estimate_index_buffer_bytes(1000);
        assert!(size > 1000 * 20); // At least 20 bytes per entry
        assert!(size < 1000 * 100); // Less than 100 bytes per entry
    }
}
