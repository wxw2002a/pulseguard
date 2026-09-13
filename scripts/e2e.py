#!/usr/bin/env python3
"""Exercise the real HTTP -> Mongo outbox -> Kafka -> Spark -> Mongo pipeline."""
import argparse
from datetime import datetime, timezone
import http.client
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


def review_alert(base_url, api_key, alert_id, status):
    path = "/api/v1/alerts/" + urllib.parse.quote(alert_id, safe="") + "/review"
    request = urllib.request.Request(
        base_url.rstrip("/") + path,
        data=json.dumps({"status": status, "note": "Verified synthetic transaction evidence", "analyst": "e2e-analyst"}).encode(),
        headers={"Content-Type": "application/json", "X-API-Key": api_key}, method="PATCH")
    with urllib.request.urlopen(request, timeout=15) as response:
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
        except (urllib.error.URLError, OSError, http.client.HTTPException, AssertionError, KeyError) as error:
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
    high_alert = next(row for row in high_alerts if row.get("rule") == "HIGH_VALUE")
    high_alert_path = "/api/v1/alerts/" + urllib.parse.quote(high_alert["id"], safe="")
    high_evidence = get_json(base, high_alert_path + "/evidence")["items"]
    require(len(high_evidence) == 1 and high_evidence[0]["transactionId"] == events[-1]["transactionId"]
            and high_evidence[0]["amountMinor"] == 600000 and high_evidence[0]["eventTime"] == timestamp,
            "HIGH_VALUE investigation returns original transaction identity, amount and event time")
    velocity_alert = next(row for row in items("alerts", card_account) if row["rule"] == "VELOCITY")
    window_evidence = get_json(base, "/api/v1/alerts/" + urllib.parse.quote(velocity_alert["id"], safe="") + "/evidence?limit=200")["items"]
    require({row["transactionId"] for row in window_evidence} == {event["transactionId"] for event in events[:6]}
            and sum(row["amountMinor"] for row in window_evidence) == 3000,
            "window investigation links exactly the six contributing original transactions")
    recovery, review_preserved, dead_letter_checked, broker_recovered = False, False, False, False
    if args.with_recovery:
        root = Path(__file__).resolve().parents[1]
        reviewed_alert = next(row for row in high_alerts if row.get("rule") == "HIGH_VALUE")
        reviewed = review_alert(base, args.api_key, reviewed_alert["id"], "INVESTIGATING")
        require(reviewed.get("status") == "INVESTIGATING" and reviewed.get("reviewedAt"), "analyst review is persisted")
        reviewed_detail = get_json(base, high_alert_path)
        review_history = reviewed_detail["reviewHistory"]
        require(reviewed_detail["reviewHistoryCount"] == 1 and len(review_history) == 1
                and review_history[0]["note"] == "Verified synthetic transaction evidence"
                and review_history[0]["analyst"] == "e2e-analyst"
                and review_history[0]["status"] == "INVESTIGATING",
                "investigation retains analyst label, rationale and status in review history")
        malformed = "{malformed-e2e-" + run
        kafka_records = [event["accountId"] + "|" + json.dumps(event) for event in (events[0], events[-1])]
        kafka_records.append("malformed-" + run + "|" + malformed)
        subprocess.run(["docker", "compose", "exec", "-T", "kafka", "/opt/kafka/bin/kafka-console-producer.sh",
                        "--bootstrap-server", "kafka:19092", "--topic", "transactions.v1",
                        "--property", "parse.key=true", "--property", "key.separator=|"],
                       input="\n".join(kafka_records) + "\n", text=True, check=True, cwd=root, timeout=60)

        def dead_letters():
            # Pass JavaScript as one argv value, never through a shell.
            query = ("JSON.stringify(db.dead_letters.find({payloadExcerpt:" + json.dumps(malformed)
                     + "}).toArray().map(doc => ({_id:doc._id,topic:doc.topic,"
                       "partition:String(doc.partition),offset:String(doc.offset),reason:doc.reason})))")
            result = subprocess.run(["docker", "compose", "exec", "-T", "mongodb", "mongosh", "--quiet",
                                     "mongodb://localhost:27017/pulseguard", "--eval", query],
                                    capture_output=True, text=True, check=True, cwd=root, timeout=30)
            return json.loads(result.stdout)

        poisoned = eventually("malformed Kafka record reaches the dead-letter collection", dead_letters, args.timeout)
        require(len(poisoned) == 1, "one dead letter for the malformed source record")
        dead_letter = poisoned[0]
        expected_id = f"{dead_letter['topic']}:{dead_letter['partition']}:{dead_letter['offset']}"
        require(dead_letter["_id"] == expected_id,
                f"dead-letter identity matches Kafka coordinates: {dead_letter['_id']} == {expected_id}")
        require(dead_letter["reason"] == "malformed JSON",
                f"dead-letter classifies malformed JSON: {dead_letter['reason']}")
        subprocess.run(["docker", "compose", "restart", "streaming"], check=True, cwd=root, timeout=120)
        # A new event is the proof that the restarted processor actually resumed.
        fresh = {**events[-1], "transactionId": f"e2e-{run}-after-restart"}
        require(post_event(base, args.api_key, fresh)[0] == 202, "accept event after Spark restart")
        eventually("Spark recovers checkpoint and processes new event", lambda: any(row.get("transactionId") == fresh["transactionId"] for row in items("alerts", high_account)), args.timeout)
        eventually("window query resumes through post-restart event", lambda: any(row.get("transactionCount") == 2 and row.get("totalAmountMinor") == 1200000 for row in items("windows", high_account)), args.timeout)
        require(bool(card_window()), "Kafka duplicate/restart preserves window count and amount")
        require(len(items("alerts", card_account)) == 2, "Kafka replay preserves deterministic alert IDs")
        replayed_review = next(row for row in items("alerts", high_account) if row["id"] == reviewed_alert["id"])
        require(replayed_review.get("status") == "INVESTIGATING" and replayed_review.get("reviewedAt") == reviewed["reviewedAt"],
                "Kafka alert replay and restart preserve analyst review status and timestamp")
        replayed_detail = get_json(base, high_alert_path)
        require(replayed_detail["reviewHistoryCount"] == 1 and replayed_detail["reviewHistory"] == review_history,
                "Kafka replay preserves the original audit history without duplicate review entries")
        recovered_dead_letters = dead_letters()
        require(len(recovered_dead_letters) == 1 and recovered_dead_letters[0]["_id"] == expected_id,
                "checkpoint recovery preserves the same single dead-letter record")
        review_preserved, dead_letter_checked = True, True
        recovery = True
        outage_event = {**events[-1], "transactionId": f"e2e-{run}-broker-outage",
                        "eventTime": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")}
        try:
            subprocess.run(["docker", "compose", "stop", "--timeout", "20", "kafka"], check=True, cwd=root, timeout=60)
            require(post_event(base, args.api_key, outage_event)[0] == 202,
                    "API durably accepts a transaction while Kafka is unavailable")
            require(get_json(base, "/api/v1/overview")["pendingDelivery"] >= 1,
                    "broker outage leaves visible pending outbox work")
            require(any(row["transactionId"] == outage_event["transactionId"] and row["deliveryStatus"] != "SENT"
                        for row in items("transactions", high_account)),
                    "transaction remains durably pending without a Kafka acknowledgement")
        finally:
            # Restore the broker even if an assertion fails, preserving useful runtime diagnostics.
            subprocess.run(["docker", "compose", "start", "kafka"], check=True, cwd=root, timeout=60)
        eventually("Kafka readiness recovers", lambda: get_json(base, "/actuator/health/readiness")["status"] == "UP", args.timeout)
        eventually("durable outbox publishes the pending transaction after broker recovery",
                   lambda: any(row["transactionId"] == outage_event["transactionId"] and row["deliveryStatus"] == "SENT"
                               for row in items("transactions", high_account)), args.timeout)
        eventually("recovered transaction produces its HIGH_VALUE alert",
                   lambda: any(row.get("transactionId") == outage_event["transactionId"] for row in items("alerts", high_account)), args.timeout)
        broker_recovered = True
    report = {"status": "passed", "runId": run, "completedAt": datetime.now(timezone.utc).isoformat(),
              "elapsedSeconds": round(time.perf_counter() - started, 3), "acceptedUniqueEvents": 7 + int(recovery) + int(broker_recovered),
              "rulesObserved": ["HIGH_VALUE", "VELOCITY", "CARD_TESTING"],
              "httpReplayChecked": True, "conflictChecked": True, "checkpointRecoveryChecked": recovery,
              "analystReviewPreservedOnReplay": review_preserved, "deadLetterIdentityChecked": dead_letter_checked,
              "originalEvidenceChecked": True,
              "brokerOutageRecoveryChecked": broker_recovered,
              "windowTransactionCount": window["transactionCount"], "windowTotalAmountMinor": window["totalAmountMinor"]}
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
