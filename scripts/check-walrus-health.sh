#!/bin/bash
# Check Walrus network health and identify down nodes
# Usage: ./scripts/check-walrus-health.sh [mainnet|testnet]

CONTEXT="${1:-mainnet}"
WALRUS_CLI="/home/evan/Documents/takopi_adventures/repos/walrus/target/debug/walrus"

echo "=== Walrus Network Health Check ==="
echo "Context: $CONTEXT"
echo "Time: $(date)"
echo ""

# Check if walrus CLI exists
if [ ! -f "$WALRUS_CLI" ]; then
    echo "ERROR: Walrus CLI not found at $WALRUS_CLI"
    exit 1
fi

# Get health status using --active-set flag, filter out log lines and ANSI codes
echo "Fetching node health (this may take a moment)..."
RAW_OUTPUT=$($WALRUS_CLI health --context "$CONTEXT" --active-set --json 2>&1)
# Strip ANSI codes and extract just the JSON (starts with {)
HEALTH_JSON=$(echo "$RAW_OUTPUT" | sed 's/\x1b\[[0-9;]*m//g' | grep -v '^\[' | grep -v '^Failed' | grep -v '^$' | sed -n '/^{/,/^}/p')

if [ -z "$HEALTH_JSON" ]; then
    echo "ERROR: Failed to get health status"
    exit 1
fi

# Validate JSON
echo "$HEALTH_JSON" | jq empty 2>/dev/null
if [ $? -ne 0 ]; then
    echo "ERROR: Invalid JSON response"
    echo "Raw output (first 500 chars):"
    echo "$HEALTH_JSON" | head -c 500
    exit 1
fi

# Parse and display results
echo ""
echo "=== Node Summary ==="
echo "$HEALTH_JSON" | jq -r '
  .healthInfo |
  group_by(
    if .healthInfo.Err then "DOWN"
    elif (.healthInfo.Ok.shardSummary.ownedShardStatus.inTransfer // 0) > 0 or
         (.healthInfo.Ok.shardSummary.ownedShardStatus.inRecovery // 0) > 0 then "DEGRADED"
    else "HEALTHY"
    end
  ) |
  map({
    status: .[0] | (
      if .healthInfo.Err then "DOWN"
      elif (.healthInfo.Ok.shardSummary.ownedShardStatus.inTransfer // 0) > 0 or
           (.healthInfo.Ok.shardSummary.ownedShardStatus.inRecovery // 0) > 0 then "DEGRADED"
      else "HEALTHY"
      end
    ),
    count: length
  }) |
  .[] |
  "\(.status): \(.count) nodes"
'

echo ""
echo "=== DOWN Nodes ==="
echo "$HEALTH_JSON" | jq -r '
  .healthInfo |
  map(select(.healthInfo.Err)) |
  if length == 0 then "None - all nodes responding"
  else
    .[] | "  - \(.nodeName) (\(.nodeUrl))"
  end
'

echo ""
echo "=== DEGRADED Nodes ==="
echo "$HEALTH_JSON" | jq -r '
  .healthInfo |
  map(select(
    .healthInfo.Ok and (
      (.healthInfo.Ok.shardSummary.ownedShardStatus.inTransfer // 0) > 0 or
      (.healthInfo.Ok.shardSummary.ownedShardStatus.inRecovery // 0) > 0
    )
  )) |
  if length == 0 then "None - no degraded nodes"
  else
    .[] | "  - \(.nodeName): inTransfer=\(.healthInfo.Ok.shardSummary.ownedShardStatus.inTransfer // 0), inRecovery=\(.healthInfo.Ok.shardSummary.ownedShardStatus.inRecovery // 0)"
  end
'

echo ""
echo "=== Shard Statistics ==="
# Count shards
TOTAL_SHARDS=$(echo "$HEALTH_JSON" | jq '[.healthInfo[].healthInfo.Ok.shardSummary.owned // 0] | add')
HEALTHY_SHARDS=$(echo "$HEALTH_JSON" | jq '[.healthInfo[] | select(.healthInfo.Ok) | .healthInfo.Ok.shardSummary.ownedShardStatus.ready // 0] | add')
DOWN_NODES=$(echo "$HEALTH_JSON" | jq '[.healthInfo[] | select(.healthInfo.Err)] | length')
DOWN_SHARDS=$(echo "$HEALTH_JSON" | jq '[.healthInfo[] | select(.healthInfo.Err) | .healthInfo.Ok.shardSummary.owned // 10] | add // 0')

echo "Total owned shards: $TOTAL_SHARDS"
echo "Ready shards: $HEALTHY_SHARDS"
echo "Down nodes: $DOWN_NODES"
echo "Estimated problematic shards: ~$((DOWN_NODES * 10))"

echo ""
echo "=== Quick Commands ==="
echo ""
echo "# Test if a blob is accessible:"
echo "timeout 60 $WALRUS_CLI read <BLOB_ID> --context $CONTEXT --size-only --json"
echo ""
echo "# Stream a specific byte range:"
echo "timeout 60 $WALRUS_CLI read <BLOB_ID> --context $CONTEXT --stream --start-byte 0 --byte-length 1000000 > /dev/null && echo 'Success' || echo 'Failed'"
echo ""
echo "# Test the problematic blob from our backfill:"
echo "timeout 60 $WALRUS_CLI read VhBj4UJ5IQEbEjSyDvW8tjkyLksDdhwr45DQ-z-_564 --context $CONTEXT --stream --start-byte 1089988144 --byte-length 5586026 > /dev/null && echo 'Success' || echo 'Failed/Timeout'"
