#!/usr/bin/env python3
"""Generate reproducible synthetic events; report measured HTTP ingestion results only."""
import argparse
import concurrent.futures
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import random
import time
import urllib.error
import urllib.request
import uuid


def post_event(base_url, api_key, event):
    request = urllib.request.Request(
        base_url.rstrip("/") + "/api/v1/transactions",
        data=json.dumps(event).encode(),
        headers={"Content-Type": "application/json", "X-API-Key": api_key},
        method="POST",
    )
    started = time.perf_counter()
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.status, (time.perf_counter() - started) * 1000, json.load(response)
    except urllib.error.HTTPError as error:
        return error.code, (time.perf_counter() - started) * 1000, error.read().decode()
    except (urllib.error.URLError, TimeoutError) as error:
        return 0, (time.perf_counter() - started) * 1000, str(error)


def generate_events(count, seed, run_id, timestamp, scenario):
    rng = random.Random(seed)
    for i in range(count):
        account = f"acct-{run_id}-{i % 24:02d}"
        amount, currency = rng.randint(1500, 85000), rng.choice(["USD", "CAD", "EUR", "GBP"])
        if scenario == "mixed" and i % 20 == 0:
            amount = rng.randint(500000, 950000)
        if scenario == "mixed" and i % 20 in range(1, 7):
            account, amount, currency = f"acct-{run_id}-card", 500, "USD"
        yield {
            "schemaVersion": 1, "transactionId": f"txn-{run_id}-{i:06d}",
            "accountId": account, "merchantId": f"merchant-{rng.randrange(8):02d}",
            "amountMinor": amount, "currency": currency,
            "country": rng.choice(["US", "CA", "GB", "DE"]),
            "channel": rng.choice(["WEB", "MOBILE", "POS"]), "eventTime": timestamp,
        }


def percentile(values, quantile):
    return round(sorted(values)[max(0, math.ceil(len(values) * quantile) - 1)], 2) if values else 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://localhost:8080")
    parser.add_argument("--api-key", default=os.environ.get("INGEST_API_KEY", "local-dev-key"))
    parser.add_argument("--count", type=int, default=120)
    parser.add_argument("--rate", type=float, default=10, help="Scheduled requests/second; 0 means unthrottled")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--run-id", default=uuid.uuid4().hex[:10])
    parser.add_argument("--timestamp", default=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"))
    parser.add_argument("--scenario", choices=["mixed", "normal"], default="mixed")
    parser.add_argument("--save-events", type=Path, help="Save exact JSONL payloads for replay")
    parser.add_argument("--replay", type=Path, help="Replay saved JSONL without changing IDs or event times")
    parser.add_argument("--report", type=Path, default=Path("artifacts/load-report.json"))
    args = parser.parse_args()
    if args.count < 1 or args.workers < 1 or args.rate < 0:
        parser.error("count/workers must be positive and rate must be nonnegative")
    events = ([json.loads(line) for line in args.replay.read_text(encoding="utf-8").splitlines() if line.strip()]
              if args.replay else list(generate_events(args.count, args.seed, args.run_id, args.timestamp, args.scenario)))
    if not events:
        parser.error("No events to send")
    if args.save_events:
        args.save_events.parent.mkdir(parents=True, exist_ok=True)
        args.save_events.write_text("".join(json.dumps(event) + "\n" for event in events), encoding="utf-8")
    started = time.perf_counter()
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
        pending = []
        for index, event in enumerate(events):
            if args.rate:
                time.sleep(max(0, started + index / args.rate - time.perf_counter()))
            pending.append(pool.submit(post_event, args.base_url, args.api_key, event))
        results = [future.result() for future in concurrent.futures.as_completed(pending)]
    elapsed = time.perf_counter() - started
    statuses = {}
    for status, _, _ in results:
        statuses[str(status)] = statuses.get(str(status), 0) + 1
    latencies = [latency for _, latency, _ in results]
    accepted = sum(status == 202 for status, _, _ in results)
    duplicates = sum(status == 202 and isinstance(body, dict) and body.get("duplicate", False)
                     for status, _, body in results)
    report = {
        "measuredAt": datetime.now(timezone.utc).isoformat(),
        "scope": "HTTP ingestion acknowledgements, not end-to-end Spark throughput",
        "runId": args.run_id, "seed": args.seed, "submitted": len(events),
        "acceptedResponses": accepted, "duplicateResponses": duplicates,
        "statusCounts": statuses, "elapsedSeconds": round(elapsed, 3),
        "acceptedResponsesPerSecond": round(accepted / elapsed, 2),
        "requestLatencyMs": {"p50": percentile(latencies, .50), "p95": percentile(latencies, .95),
                             "p99": percentile(latencies, .99), "max": round(max(latencies), 2)},
        "failures": [{"status": status, "detail": str(body)[:500]}
                     for status, _, body in results if status != 202][:10],
    }
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))
    return 0 if accepted == len(events) else 1


if __name__ == "__main__":
    raise SystemExit(main())
