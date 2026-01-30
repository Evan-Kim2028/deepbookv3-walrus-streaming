# Walrus Sliver Prediction & Node Health Troubleshooting

## Overview

This document describes the sliver prediction system used for adaptive fetching from Walrus storage, the current node outage issues affecting blob retrieval, and how to troubleshoot these problems.

## Background: Walrus Erasure Coding

Walrus uses **RedStuff erasure coding** to distribute data across storage nodes:

- **1000 shards** distributed across ~100 storage nodes (~10 shards per node)
- Data is arranged into a **334 x 667 matrix** of symbols
- **Primary slivers**: 334 slivers (each contains a row of 667 symbols)
- **Secondary slivers**: 667 slivers (each contains a column of 334 symbols)
- **Symbol size**: 256 bytes
- Only need ~1/3 of slivers to reconstruct any byte range

### Sliver-to-Shard Mapping

Each blob has a **rotation offset** computed from its blob_id:

```
rotation_offset = blob_id_bytes % 1000
shard = (sliver_index + rotation_offset) % 1000
```

This means different blobs map their slivers to different shards, providing fault tolerance.

### Byte-to-Sliver Mapping

For a given byte offset:

```
symbol_index = byte_offset / 256
primary_sliver = symbol_index / 667    (which row)
secondary_sliver = symbol_index % 667  (which column)
```

Each primary sliver covers approximately **170,752 bytes** (667 * 256).

## Current Issue: Node Outages

### Observed Down Nodes (as of 2026-01-30 07:12 EST)

Run `./scripts/check-walrus-health.sh mainnet` for current status.

**Current snapshot: 16 nodes DOWN (~160 problematic shards, 16% of network)**

| Node Name | Node URL | Status |
|-----------|----------|--------|
| DECOIN | 116.163.20.31:9185 | DOWN |
| doubleup-walrus-node | doubleup-walrus.com:9185 | DOWN |
| hoh.zone | mainnet-walrus.hoh.zone:9185 | DOWN |
| StakeSquid | sm1-walrus-mainnet.stakesquid.com:9186 | DOWN |
| Nodz | storage.walrus.mainnet.nodz.io:9185 | DOWN |
| Astro-Stakers | walrus-main.astrostakers.com:9185 | DOWN |
| Bucket Protocol | walrus-main.bucketprotocol.io:9185 | DOWN |
| chainup_walrus | walrus-mainnet-01.chainup.net:9185 | DOWN |
| Nami Cloud | walrus-mainnet-node.nami.cloud:9185 | DOWN |
| TrustedPoint | walrus-mainnet-storage.trusted-point.com:9185 | DOWN |
| Everstake | walrus-mainnet.everstake.one:9185 | DOWN |
| MINARA | walrus-mainnet.trustepoch.com:9185 | DOWN |
| EOSUSA-STOR01 | walrus.eosusa.io:9185 | DOWN |
| AlphaFi | walrus.mainnet.alphafi.xyz:9185 | DOWN |
| pinata-bot | walrus.pinatabot.com:9185 | DOWN |
| StakeCapital | walrus.vol-0e0c175f9734fdf77.prod.stake.capital:9185 | DOWN |
| Brightlystake | - | DEGRADED (1 shard in recovery) |

**Note**: Node availability fluctuates. During the backfill run, we observed 5-11 nodes down at various times.

### Impact on Blob Retrieval

For blob `VhBj4UJ5IQEbEjSyDvW8tjkyLksDdhwr45DQ-z-_564`:
- **Rotation offset**: 470
- **Primary slivers 0-333** map to **shards 470-803**
- With current down nodes, **8 problematic primary slivers** identified
- Only **3.3% of blob is "safe"** (doesn't require down shards)

When a byte range requires slivers from down nodes, the CLI must reconstruct from secondary slivers or other redundant data. If too many nodes are down, reconstruction becomes impossible and requests timeout.

## Troubleshooting Commands

### Quick Health Check Script

```bash
# Run the health check script (recommended)
./scripts/check-walrus-health.sh mainnet
```

### Check Node Health via Walrus CLI

```bash
# Get full node health status (JSON output)
/home/evan/Documents/takopi_adventures/repos/walrus/target/debug/walrus \
  health \
  --context mainnet \
  --active-set \
  --json

# Pretty print with jq (filter out log lines first)
/home/evan/Documents/takopi_adventures/repos/walrus/target/debug/walrus \
  health \
  --context mainnet \
  --active-set \
  --json 2>&1 | sed 's/\x1b\[[0-9;]*m//g' | sed -n '/^{/,/^}/p' | jq '.'

# List only DOWN nodes
/home/evan/Documents/takopi_adventures/repos/walrus/target/debug/walrus \
  health \
  --context mainnet \
  --active-set \
  --json 2>&1 | sed 's/\x1b\[[0-9;]*m//g' | sed -n '/^{/,/^}/p' | \
  jq -r '.healthInfo[] | select(.healthInfo.Err) | .nodeName'
```

### Check Specific Blob Size

```bash
# Get blob size without downloading
/home/evan/Documents/takopi_adventures/repos/walrus/target/debug/walrus \
  read <BLOB_ID> \
  --context mainnet \
  --size-only \
  --json
```

### Stream a Specific Byte Range

```bash
# Stream bytes 1000000-2000000 from a blob
/home/evan/Documents/takopi_adventures/repos/walrus/target/debug/walrus \
  read <BLOB_ID> \
  --context mainnet \
  --stream \
  --start-byte 1000000 \
  --byte-length 1000000
```

### Test if a Specific Range is Accessible

```bash
# Try to read a problematic range with timeout
timeout 60 /home/evan/Documents/takopi_adventures/repos/walrus/target/debug/walrus \
  read VhBj4UJ5IQEbEjSyDvW8tjkyLksDdhwr45DQ-z-_564 \
  --context mainnet \
  --stream \
  --start-byte 1089988144 \
  --byte-length 5586026

# Exit code 124 = timeout (range is inaccessible)
```

## Reproducing the Issue

### 1. Start the Streaming Backfill

```bash
cd /home/evan/Documents/takopi_adventures/projects/deepbookv3

rm -f backfill_progress.state

DATABASE_URL="postgres://postgres:postgrespw@localhost:5433/deepbook" \
RUST_LOG=info \
CHECKPOINT_STORAGE=walrus \
CHECKPOINT_CACHE_ENABLED=false \
./target/release/deepbook-indexer \
  --env mainnet \
  --walrus-cli-path /home/evan/Documents/takopi_adventures/repos/walrus/target/debug/walrus \
  --walrus-cli-context mainnet \
  --walrus-cli-timeout-secs 120 \
  --walrus-cli-range-concurrency 2 \
  --packages deepbook \
  --packages deepbook-margin \
  --first-checkpoint 239495050 \
  --last-checkpoint 239510000 \
  --walrus-cli-backfill \
  > ./logs/streaming_backfill.log 2>&1 &
```

### 2. Monitor Progress

```bash
# Watch the log
tail -f ./logs/streaming_backfill.log

# Check for timeouts
grep "timed out" ./logs/streaming_backfill.log

# Check processing speed
grep "processed.*checkpoints" ./logs/streaming_backfill.log | tail -10
```

### 3. Observe the Failure Pattern

The backfill will eventually hit a byte range that maps to down nodes:

```
WARN walrus cli range read failed (attempt 1/6): walrus cli timed out after 120s
WARN walrus cli range read failed (attempt 2/6): walrus cli timed out after 120s
...
WARN splitting walrus cli range read (22344105 bytes) into 11172052 + 11172053
```

The system will:
1. Retry 6 times with exponential backoff
2. Split the range in half and retry each half
3. Continue splitting until min_split_bytes (4MB) is reached
4. Eventually fail if reconstruction is impossible

## Sliver Prediction System

### How It Works

1. **Health Tracking**: Polls node health every 5 minutes or after 3+ timeouts
2. **Shard Mapping**: Maps down/degraded nodes to their shard IDs
3. **Blob Analysis**: For each blob, computes which primary slivers map to problematic shards
4. **Range Classification**: Classifies byte ranges as Safe, Low, High, or Critical risk

### Risk Levels

| Risk Level | Problematic Slivers | Action |
|------------|---------------------|--------|
| Safe | 0 | Aggressive concurrency (12x) |
| Low | 1-2 | Standard concurrency (6x) |
| High | 3-5 | Conservative fetching |
| Critical | 6+ | High chance of timeout |

### Log Examples

```
INFO sliver predictor initialized with 77 problematic shards

INFO blob VhBj4UJ5IQEbEjSyDvW8tjkyLksDdhwr45DQ-z-_564 analysis:
     3.3% safe, 1 safe ranges, 2 risky ranges,
     8 problematic primary slivers (rotation=470)

INFO stream range classification for blob ...:
     0 safe, 0 low, 0 high, 1 critical
```

## Calculating Problematic Ranges

### Example: Blob with rotation=470

Given:
- Rotation offset: 470
- Problematic shards: 257, 258, 259, ... (from down nodes)

To find which primary slivers are affected:

```
For shard S to be problematic for this blob:
  sliver_index = (S - 470 + 1000) % 1000

If sliver_index < 334, it's a primary sliver and affects a contiguous byte range.
```

For shard 257:
```
sliver_index = (257 - 470 + 1000) % 1000 = 787
```
787 > 334, so this is a secondary sliver (doesn't create a "risky" byte range directly).

For shard 500:
```
sliver_index = (500 - 470 + 1000) % 1000 = 30
```
30 < 334, so primary sliver 30 is problematic.
Affected bytes: `30 * 667 * 256` to `31 * 667 * 256` = bytes 5,125,120 to 5,295,872

## Recommendations

### Short-term Workarounds

1. **Skip problematic ranges**: Modify backfill to skip checkpoints in problematic byte ranges and return later

2. **Use cache mode**: Download full blobs first (slower but more reliable):
   ```bash
   CHECKPOINT_CACHE_ENABLED=true
   ```

3. **Reduce concurrency**: Lower concurrent requests to reduce load on remaining nodes

4. **Increase timeout**: Give more time for reconstruction:
   ```bash
   --walrus-cli-timeout-secs 300
   ```

### Long-term Solutions

1. **Wait for node recovery**: The down nodes may come back online
2. **Report to Walrus team**: File issues for consistently down operators
3. **Implement skip-and-retry**: Track failed ranges and retry periodically

## File Locations

- **Sliver prediction code**: `crates/indexer/src/sliver_prediction.rs`
- **Node health tracking**: `crates/indexer/src/node_health.rs`
- **Walrus storage integration**: `crates/indexer/src/walrus_storage.rs`
- **Forked Walrus CLI**: `/home/evan/Documents/takopi_adventures/repos/walrus/target/debug/walrus`
- **Backfill logs**: `./logs/streaming_backfill.log`

## Performance Observations

When network is healthy (< 5% problematic shards):
- **Speed**: 10-18 checkpoints/sec
- **Throughput**: 1-2.5 MB/s

When network is degraded (7-11% problematic shards):
- **Speed**: 3-6 checkpoints/sec (highly variable)
- **Throughput**: 0.3-0.7 MB/s
- **Frequent timeouts** on ranges hitting down shards

## Contact

For Walrus network issues, check:
- Walrus Discord
- Walrus GitHub issues
- Sui Foundation infrastructure team
