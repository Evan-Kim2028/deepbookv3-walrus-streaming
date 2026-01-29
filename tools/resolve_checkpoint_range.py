#!/usr/bin/env python3
import argparse
import json
import time
import urllib.request


def rpc_call(rpc_url: str, method: str, params: list):
    payload = json.dumps(
        {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    ).encode("utf-8")
    req = urllib.request.Request(
        rpc_url, data=payload, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.load(resp)
    if "error" in data:
        raise RuntimeError(data["error"])
    return data["result"]


def checkpoint_timestamp_ms(rpc_url: str, seq: int) -> int:
    result = rpc_call(rpc_url, "sui_getCheckpoint", [str(seq)])
    return int(result["timestampMs"])


def latest_checkpoint(rpc_url: str) -> int:
    return int(rpc_call(rpc_url, "sui_getLatestCheckpointSequenceNumber", []))


def find_checkpoint_at_or_after(rpc_url: str, target_ms: int, low: int, high: int) -> int:
    while low < high:
        mid = (low + high) // 2
        ts = checkpoint_timestamp_ms(rpc_url, mid)
        if ts < target_ms:
            low = mid + 1
        else:
            high = mid
    return low


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Resolve a checkpoint range by time window (binary search on timestamp)."
    )
    parser.add_argument(
        "--rpc",
        default="https://fullnode.mainnet.sui.io:443",
        help="Sui JSON-RPC endpoint (default: mainnet fullnode)",
    )
    parser.add_argument(
        "--days",
        type=float,
        default=7.0,
        help="Lookback window in days (default: 7)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print JSON output instead of human-readable text",
    )
    args = parser.parse_args()

    end_checkpoint = latest_checkpoint(args.rpc)
    now_ms = int(time.time() * 1000)
    target_ms = now_ms - int(args.days * 24 * 60 * 60 * 1000)

    start_checkpoint = find_checkpoint_at_or_after(
        args.rpc, target_ms, 0, end_checkpoint
    )

    payload = {
        "rpc": args.rpc,
        "start_checkpoint": start_checkpoint,
        "end_checkpoint": end_checkpoint,
        "target_timestamp_ms": target_ms,
        "lookback_days": args.days,
        "resolved_at_ms": now_ms,
    }

    if args.json:
        print(json.dumps(payload))
    else:
        print(
            "start_checkpoint={start} end_checkpoint={end} target_timestamp_ms={target}".format(
                start=start_checkpoint, end=end_checkpoint, target=target_ms
            )
        )


if __name__ == "__main__":
    main()
