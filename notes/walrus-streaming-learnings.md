# Streaming Sui Checkpoint Data from Walrus

A guide for engineers looking to efficiently read Sui blockchain checkpoint data stored in Walrus decentralized storage.

## Repository

**GitHub:** https://github.com/Evan-Kim2028/deepbookv3-walrus-streaming

This fork of DeepBook V3 includes:
- `walrus-streamer` - A Rust tool for streaming checkpoint data from Walrus archival blobs
- Modified indexer with Walrus storage backend support

---

## Background

### The Problem

Sui checkpoint data is archived to Walrus blob storage by the [Walrus Archival Service](https://walrus-sui-archival.mainnet.walrus.space). Each blob contains thousands of checkpoints and can be hundreds of MB to several GB.

When building an indexer, you often need to:
- Read specific checkpoint ranges (not entire blobs)
- Avoid loading multi-GB blobs into memory
- Resume interrupted downloads
- Process checkpoints in parallel across workers

### Initial Assumption (Wrong)

We initially thought we'd need to fork the Walrus CLI to add streaming/byte-range support.

### What We Discovered

The Walrus CLI **already supports byte-range reads**:

```bash
walrus read <blob_id> --start-byte 1000000 --byte-length 50000
```

This was added in PRs [#2708](https://github.com/MystenLabs/walrus/pull/2708) and [#2724](https://github.com/MystenLabs/walrus/pull/2724).

The HTTP aggregator also supports standard range requests:
```bash
curl -H "Range: bytes=1000000-1050000" https://aggregator.walrus-mainnet.walrus.space/v1/blobs/<blob_id>
```

---

## What We Built

Instead of forking Walrus, we built a **wrapper tool** that leverages existing capabilities.

### walrus-streamer

A CLI tool that:
1. Fetches blob metadata from the archival service
2. Parses the checkpoint index from the blob footer
3. Plans byte ranges for requested checkpoints
4. Streams data via CLI subprocess or HTTP
5. Decodes checkpoints and optionally filters by package

### Key Features

| Feature | Description |
|---------|-------------|
| Byte-range streaming | Uses `walrus read --start-byte --byte-length` |
| Index parsing | Reads blob footer to get checkpoint offsets |
| Range coalescing | Groups nearby checkpoints to reduce read ops |
| Resumable downloads | Tracks progress in JSON state file |
| Parallel chunking | Split work across multiple workers |
| Dual transport | CLI (offline) or HTTP (aggregator) |

---

## Archival Blob Format

Walrus archival blobs have a specific structure:

```
+----------------------------------+
| Checkpoint 1 (BCS encoded)       |
+----------------------------------+
| Checkpoint 2 (BCS encoded)       |
+----------------------------------+
| ...                              |
+----------------------------------+
| Checkpoint N (BCS encoded)       |
+----------------------------------+
| Index Entries                    |
+----------------------------------+
| Footer (24 bytes)                |
+----------------------------------+
```

### Footer Structure (24 bytes)
| Field | Size | Description |
|-------|------|-------------|
| Magic | 4 bytes | `0x574c4244` ("WLBD") |
| Version | 4 bytes | Format version |
| Index Offset | 8 bytes | Byte offset where index starts |
| Entry Count | 4 bytes | Number of checkpoints |
| CRC | 4 bytes | Checksum |

### Index Entry Structure
| Field | Size | Description |
|-------|------|-------------|
| Name Length | 4 bytes | Length of checkpoint number string |
| Name | variable | Checkpoint number as UTF-8 string |
| Offset | 8 bytes | Byte offset of checkpoint data |
| Length | 8 bytes | Size of checkpoint data |
| CRC | 4 bytes | Entry checksum |

---

## Usage Examples

### Basic: Stream latest checkpoints
```bash
walrus-streamer \
  --walrus-cli-path ~/.cargo/bin/walrus \
  --transport cli \
  --index-source cli
```

### Specific checkpoint range
```bash
walrus-streamer \
  --walrus-cli-path ~/.cargo/bin/walrus \
  --start-checkpoint 95000000 \
  --end-checkpoint 95001000 \
  --transport cli \
  --index-source cli
```

### HTTP transport (uses aggregator)
```bash
walrus-streamer \
  --transport http \
  --index-source aggregator \
  --aggregator-url https://aggregator.walrus-mainnet.walrus.space
```

### Parallel processing (4 workers)
```bash
# Worker 0
walrus-streamer --chunk-count 4 --chunk-index 0 --walrus-cli-path ~/.cargo/bin/walrus

# Worker 1
walrus-streamer --chunk-count 4 --chunk-index 1 --walrus-cli-path ~/.cargo/bin/walrus

# etc...
```

### Filter for specific package events
```bash
walrus-streamer \
  --package-id 0xdee9006cf1d85e6c34653a4998c05feb9ff20a4ed1e5c1503cb26c5c4a0cd167 \
  --walrus-cli-path ~/.cargo/bin/walrus
```

### Resumable download
```bash
walrus-streamer \
  --resume-path ./progress.json \
  --walrus-cli-path ~/.cargo/bin/walrus
```

---

## Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `--transport` | `cli` | `cli` or `http` |
| `--index-source` | `cli` | `cli`, `aggregator`, `file`, or `none` |
| `--walrus-cli-path` | required for cli | Path to walrus binary |
| `--walrus-context` | `mainnet` | Walrus network context |
| `--aggregator-url` | mainnet aggregator | For HTTP transport |
| `--archival-url` | mainnet archival | Blob metadata source |
| `--start-checkpoint` | blob start | First checkpoint to process |
| `--end-checkpoint` | blob end | Last checkpoint to process |
| `--limit` | 0 (unlimited) | Max checkpoints to process |
| `--from-latest` | false | Start from newest checkpoint |
| `--blob-id` | latest | Specific blob to process |
| `--blob-count` | 1 | Number of recent blobs |
| `--concurrency` | 1 | Parallel blob processing |
| `--range-concurrency` | 1 | Parallel range reads per blob |
| `--coalesce-gap-bytes` | 0 | Max gap for range coalescing |
| `--max-range-bytes` | 32MB | Max coalesced range size |
| `--chunk-count` | 0 | Split blob into N chunks |
| `--chunk-index` | none | Process specific chunk |
| `--resume-path` | none | State file for resumability |
| `--stream-ranges` | false | Decode while streaming (low memory) |
| `--raw-download` | false | Download without decoding |

---

## Performance

From our benchmarking:
- **CLI transport:** ~20-50 MB/s (depends on storage node load)
- **HTTP transport:** Similar throughput via aggregator
- **Index extraction:** Minimal overhead (only reads blob tail)
- **Range coalescing:** Reduces read operations significantly for sparse selections

---

## Lessons Learned

1. **Check existing features first** - We spent time planning a fork before discovering the capability already existed upstream.

2. **Wrapper tools are often sufficient** - Instead of forking, wrapping existing CLIs can be simpler and stays compatible with upstream updates.

3. **Archival blob format is specific** - The footer/index format is specific to Sui checkpoint archival, not a general Walrus feature.

4. **MystenLabs requires SSO** - To interact with their GitHub repos via CLI, you need to authorize at https://github.com/orgs/MystenLabs/sso

---

## Related Upstream Work

| PR/Issue | Description |
|----------|-------------|
| [#2801](https://github.com/MystenLabs/walrus/pull/2801) | Aggregator streaming endpoint |
| [#2724](https://github.com/MystenLabs/walrus/pull/2724) | Aggregator byte range reads |
| [#2708](https://github.com/MystenLabs/walrus/pull/2708) | CLI byte range support |
| [#2631](https://github.com/MystenLabs/walrus/pull/2631) | Quilt streaming prototype |
| [#1414](https://github.com/MystenLabs/walrus/issues/1414) | Async uploading discussion |

---

## Project Structure

```
deepbookv3-walrus-streaming/
├── crates/
│   ├── indexer/                    # Modified DeepBook indexer
│   │   └── src/
│   │       ├── walrus_storage.rs   # Walrus backend for checkpoints
│   │       └── main.rs             # CLI with Walrus options
│   └── walrus-streamer/            # Streaming tool
│       └── src/main.rs             # ~2000 lines with tests
├── notes/
│   └── walrus-streaming-learnings.md  # This document
└── WALRUS_DOWNLOADER_GUIDE.md      # Additional documentation
```

---

## Contact

Built while working on DeepBook V3 indexing infrastructure.

- GitHub: [@Evan-Kim2028](https://github.com/Evan-Kim2028)
