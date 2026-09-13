#!/usr/bin/env python3
"""Exercise the real HTTP -> Mongo outbox -> Kafka -> Spark -> Mongo pipeline."""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

from load_generator import post_event


def get_json(base_url, path):
    with urllib.request.urlopen(base_url.rstrip("/") + path, timeout=15) as response:
        return json.load(response)


def eventually(description, check, timeout):
    deadline = time.monotonic() + timeout
    last_error = "condition not satisfied"
    while time.monotonic() < deadline:
        try:
            result = check()
            if result:
                print(f"PASS {description}", flush=True)
                return result
        except (urllib.error.URLError, TimeoutError, AssertionError, KeyError) as error:
            last_error = str(error)
        time.sleep(2)
    raise AssertionError(f"Timed out: {description}; last error: {last_error}")


def require(condition, description):
    if not condition:
        raise AssertionError(description)
    print(f"PASS {description}", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://localhost:8080")
    parser.add_argument("--api-key", default=os.environ.get("INGEST_API_KEY", "local-dev-key"))
    parser.add_argument("--timeout", type=int, default=240)
    parser.add_argument("--with-recovery", action="store_true", help="Inject a Kafka duplicate and restart Compose Spark with its checkpoint")
    parser.add_argument("--report", type=Path, default=Path("artifacts/e2e-report.json"))
    args = parser.parse_args()
    started = time.perf_counter()
    base = args.base_url
    eventually("API readiness", lambda: get_json(base, "/actuator/health/readiness")["status"] == "UP", args.timeout)
    run = uuid.uuid4().hex[:12]
    timestamp = datetime.now(timezone.utc).replace(second=0, microsecond=0).isoformat().replace("+00:00", "Z")
    card_account, high_account = f"e2e-card-{run}", f"e2e-high-{run}"
    template = {"schemaVersion": 1, "merchantId": "merchant-e2e", "currency": "USD", "country": "US", "channel": "WEB", "eventTime": timestamp}
    events = [{**template, "transactionId": f"e2e-{run}-{i}", "accountId": card_account, "amountMinor": 500} for i in range(6)]
    events.append({**template, "transactionId": f"e2e-{run}-high", "accountId": high_account, "amountMinor": 600000})
    require(post_event(base, "incorrect-api-key", events[0])[0] == 401, "reject missing/incorrect authentication")
    invalid = {**events[0], "amountMinor": -1}
    require(post_event(base, args.api_key, invalid)[0] == 400, "reject invalid transaction")
    for event in events:
        status, _, body = post_event(base, args.api_key, event)
        require(status == 202, f"accept {event['transactionId']}: HTTP {status}")
    status, _, body = post_event(base, args.api_key, events[0])
    require(status == 202 and body.get("duplicate") is True, "exact HTTP replay is idempotent")
    require(post_event(base, args.api_key, {**events[0], "amountMinor": 501})[0] == 409, "conflicting transaction ID is rejected")

    def items(resource, account):
        return get_json(base, f"/api/v1/{resource}?" + urllib.parse.urlencode({"accountId": account, "limit": 200}))["items"]

    def card_window():
        for row in items("windows", card_account):
            if row.get("currency") == "USD" and row.get("transactionCount") == 6 and row.get("totalAmountMinor") == 3000:
                return row
        return False

    eventually("HIGH_VALUE alert materialized", lambda: any(row.get("rule") == "HIGH_VALUE" for row in items("alerts", high_account)), args.timeout)
    window = eventually("six-event window with exact integer amount 3000", card_window, args.timeout)
    eventually("VELOCITY and CARD_TESTING alerts materialized", lambda: {"VELOCITY", "CARD_TESTING"}.issubset({row.get("rule") for row in items("alerts", card_account)}), args.timeout)
    require(len(items("transactions", card_account)) == 6, "HTTP duplicate creates no additional transaction")
    high_alerts = items("alerts", high_account)
    require(sum(row.get("rule") == "HIGH_VALUE" for row in high_alerts) == 1, "one HIGH_VALUE alert per immutable transaction")
    recovery = False
    if args.with_recovery:
        root = Path(__file__).resolve().parents[1]
        replay = events[0]
        subprocess.run(["docker", "compose", "exec", "-T", "kafka", "/opt/kafka/bin/kafka-console-producer.sh",
                        "--bootstrap-server", "kafka:19092", "--topic", "transactions.v1",
                        "--property", "parse.key=true", "--property", "key.separator=|"],
                       input=replay["accountId"] + "|" + json.dumps(replay) + "\n", text=True, check=True, cwd=root, timeout=60)
        # Give the duplicate a micro-batch, then restart using the same persisted checkpoints.
        time.sleep(8)
        subprocess.run(["docker", "compose", "restart", "streaming"], check=True, cwd=root, timeout=120)
        # A new event is the proof that the restarted processor actually resumed.
        fresh = {**events[-1], "transactionId": f"e2e-{run}-after-restart"}
        require(post_event(base, args.api_key, fresh)[0] == 202, "accept event after Spark restart")
        eventually("Spark recovers checkpoint and processes new event", lambda: any(row.get("transactionId") == fresh["transactionId"] for row in items("alerts", high_account)), args.timeout)
        eventually("window query resumes through post-restart event", lambda: any(row.get("transactionCount") == 2 and row.get("totalAmountMinor") == 1200000 for row in items("windows", high_account)), args.timeout)
        require(bool(card_window()), "Kafka duplicate/restart preserves window count and amount")
        require(len(items("alerts", card_account)) == 2, "Kafka replay preserves deterministic alert IDs")
        recovery = True
    report = {"status": "passed", "runId": run, "completedAt": datetime.now(timezone.utc).isoformat(),
              "elapsedSeconds": round(time.perf_counter() - started, 3), "acceptedUniqueEvents": 7 + int(recovery),
              "rulesObserved": ["HIGH_VALUE", "VELOCITY", "CARD_TESTING"],
              "httpReplayChecked": True, "conflictChecked": True, "checkpointRecoveryChecked": recovery,
              "windowTransactionCount": window["transactionCount"], "windowTotalAmountMinor": window["totalAmountMinor"]}
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
