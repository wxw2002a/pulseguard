#!/usr/bin/env python3
"""Reproduce a payment-risk team's evidence, concurrency and disposition workflow.

Uses the real HTTP -> Kafka -> Spark -> MongoDB stack, synthetic transactions,
and two concurrent HTTP clients. No direct database writes or mock alerts.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import threading
import urllib.error
import urllib.parse
import urllib.request
import uuid

from e2e import eventually, get_json, require
from load_generator import post_event


def command(base, key, alert_id, body):
    url = base.rstrip("/") + "/api/v1/alerts/" + urllib.parse.quote(alert_id, safe="") + "/review"
    request = urllib.request.Request(url, data=json.dumps(body).encode(), method="PATCH",
                                     headers={"Content-Type": "application/json", "X-API-Key": key})
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as error:
        return error.code, json.load(error)


def run_scenario(base, key, timeout):
    eventually("API and dependencies are ready", lambda: get_json(base, "/actuator/health/readiness")["status"] == "UP", timeout)
    run_id = uuid.uuid4().hex[:12]
    account = "ops-" + run_id
    high_account = "ops-high-" + run_id
    timestamp = datetime.now(timezone.utc).replace(second=0, microsecond=0).isoformat().replace("+00:00", "Z")
    template = {"schemaVersion": 1, "accountId": account, "merchantId": "shop-operations",
                "currency": "USD", "country": "US", "channel": "WEB", "eventTime": timestamp}
    # A normal payment shares the minute with six small card-testing attempts.
    # Only the small payments are evidence of CARD_TESTING.
    events = [{**template, "transactionId": f"ops-{run_id}-{index}", "amountMinor": amount}
              for index, amount in enumerate([25000, 500, 500, 500, 500, 500, 500])]
    events.append({**template, "accountId": high_account, "transactionId": f"ops-{run_id}-high", "amountMinor": 700000})
    for event in events:
        require(post_event(base, key, event)[0] == 202, "accepted synthetic event " + event["transactionId"])

    def alerts_for(target):
        return get_json(base, "/api/v1/alerts?" + urllib.parse.urlencode({"accountId": target, "limit": 200}))["items"]

    def find_rule(target, rule):
        return next((alert for alert in alerts_for(target) if alert["rule"] == rule), None)

    card = eventually("card-testing case available", lambda: find_rule(account, "CARD_TESTING"), timeout)
    high = eventually("high-value case available", lambda: find_rule(high_account, "HIGH_VALUE"), timeout)
    evidence_path = "/api/v1/alerts/" + urllib.parse.quote(card["id"], safe="") + "/evidence"
    before = get_json(base, evidence_path)
    ids = {item["transactionId"] for item in before["items"]}
    require(before["provenance"] == "PINNED_DETECTION" and before["complete"]
            and len(ids) == before["evidenceCount"] and 5 <= len(ids) <= 6
            and ids.issubset({event["transactionId"] for event in events[1:7]})
            and all(item["amountMinor"] <= 1000 for item in before["items"]),
            "card-testing evidence contains only the triggering small payments")

    later = {**template, "transactionId": f"ops-{run_id}-later", "amountMinor": 500}
    require(post_event(base, key, later)[0] == 202, "accepted later same-window payment")
    eventually("later payment changes the aggregate window", lambda: any(
        window["transactionCount"] == 8 for window in get_json(base, "/api/v1/windows?accountId=" + account)["items"]), timeout)
    after = get_json(base, evidence_path)
    require({item["transactionId"] for item in after["items"]} == ids
            and after["evidenceCount"] == before["evidenceCount"],
            "subsequent same-window payments do not rewrite initial detection evidence")

    barrier = threading.Barrier(2)
    claims = [{"action": "CLAIM", "expectedVersion": high.get("version", 0),
               "operationId": f"{run_id}-claim-{name}", "analyst": name,
               "note": "Inspect original payment and merchant context"} for name in ("analyst-alice", "analyst-bob")]

    def race(body):
        barrier.wait(timeout=10)
        return body, command(base, key, high["id"], body)

    with ThreadPoolExecutor(max_workers=2) as pool:
        responses = list(pool.map(race, claims))
    require(sorted(result[0] for _, result in responses) == [200, 409],
            "two simultaneous claims have exactly one winner and one conflict")
    winning, (_, claimed) = next(pair for pair in responses if pair[1][0] == 200)
    losing = next(body for body, result in responses if result[0] == 409)
    status, retried = command(base, key, high["id"], winning)
    require(status == 200 and retried["version"] == claimed["version"]
            and retried["reviewHistoryCount"] == 1,
            "retrying a successful claim appends no duplicate audit record")
    require(command(base, key, high["id"], {**winning, "note": "Changed command payload"})[0] == 409,
            "operation ID cannot be reused for a different command")
    require(command(base, key, high["id"], {**losing, "action": "COMMENT", "expectedVersion": claimed["version"],
            "operationId": f"{run_id}-wrong-owner"})[0] == 409, "another operator cannot change the owned case")
    stale = {**winning, "action": "RESOLVE", "disposition": "CONFIRMED_RISK", "operationId": f"{run_id}-stale"}
    require(command(base, key, high["id"], stale)[0] == 409, "stale decision is rejected before it overwrites current work")
    resolve = {**winning, "action": "RESOLVE", "expectedVersion": claimed["version"],
               "operationId": f"{run_id}-resolve", "disposition": "CONFIRMED_RISK",
               "note": "Synthetic exercise: manual investigation confirms the risk signal"}
    missing_label = {key: value for key, value in resolve.items() if key != "disposition"}
    require(command(base, key, high["id"], missing_label)[0] == 400, "resolution requires a disposition")
    status, resolved = command(base, key, high["id"], resolve)
    require(status == 200 and resolved["status"] == "RESOLVED" and resolved["disposition"] == "CONFIRMED_RISK"
            and resolved["reviewHistoryCount"] == 2, "owned investigation closes with a reasoned outcome")
    retry_status, resolved_retry = command(base, key, high["id"], resolve)
    require(retry_status == 200 and resolved_retry["reviewHistoryCount"] == 2,
            "retrying resolution does not count a second decision")

    closed_queue = get_json(base, "/api/v1/alerts?" + urllib.parse.urlencode(
        {"accountId": high_account, "status": "RESOLVED", "owner": winning["analyst"], "limit": 1}))
    require([item["id"] for item in closed_queue["items"]] == [high["id"]],
            "status and owner filtering find the resolved case")
    outcomes = get_json(base, "/api/v1/outcomes")
    require(any(row["rule"] == "HIGH_VALUE" and row["confirmedRisk"] >= 1 for row in outcomes["byRule"]),
            "rule outcomes include the confirmed analyst label")

    reopen = {**winning, "action": "REOPEN", "expectedVersion": resolved["version"],
              "operationId": f"{run_id}-reopen", "note": "New merchant context requires another investigation"}
    status, reopened = command(base, key, high["id"], reopen)
    require(status == 200 and reopened["status"] == "INVESTIGATING" and reopened.get("disposition") is None,
            "reopening clears the current disposition and preserves history")
    final_command = {**resolve, "expectedVersion": reopened["version"], "operationId": f"{run_id}-final",
                     "note": "Synthetic follow-up complete; retained for the review demonstration"}
    status, final = command(base, key, high["id"], final_command)
    require(status == 200 and final["reviewHistoryCount"] == 4, "reopened case can be resolved with a complete four-action audit")
    # Leave a contrasting, explicitly synthetic analyst label for the outcome view.
    card_claim = {**winning, "expectedVersion": card.get("version", 0), "operationId": f"{run_id}-card-claim"}
    status, owned_card = command(base, key, card["id"], card_claim)
    require(status == 200, "claimed the card-testing signal")
    card_resolve = {**resolve, "expectedVersion": owned_card["version"], "operationId": f"{run_id}-card-resolve",
                    "disposition": "FALSE_POSITIVE", "note": "Synthetic fixture: merchant confirms this was an authorized payment test"}
    require(command(base, key, card["id"], card_resolve)[0] == 200, "false-positive label retained for rule tuning")
    return {"status": "passed", "runId": run_id, "completedAt": datetime.now(timezone.utc).isoformat(),
            "dataset": "synthetic payment events; analyst outcomes are scripted, not fraud ground truth",
            "acceptedUniqueEvents": 9, "highValueAlertId": high["id"], "cardTestingAlertId": card["id"],
            "pinnedEvidenceIds": sorted(ids), "concurrentClaims": [200, 409], "auditActions": final["reviewHistoryCount"],
            "pinnedEvidenceChecked": True, "ownershipChecked": True, "idempotentCommandsChecked": True,
            "staleDecisionChecked": True, "dispositionsChecked": True, "reopenChecked": True,
            "queueFiltersChecked": True, "outcomes": get_json(base, "/api/v1/outcomes")}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://localhost:8080")
    parser.add_argument("--api-key", default=os.environ.get("INGEST_API_KEY", "local-dev-key"))
    parser.add_argument("--timeout", type=int, default=300)
    parser.add_argument("--report", type=Path, default=Path("artifacts/analyst-scenario.json"))
    args = parser.parse_args()
    report = run_scenario(args.base_url, args.api_key, args.timeout)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))
