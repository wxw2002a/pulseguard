#!/usr/bin/env python3
"""Package the shared dashboard and a verified CI snapshot for GitHub Pages."""

import argparse
import json
import re
import shutil
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
STATIC = ROOT / "services/api/src/main/resources/static"


def validate_snapshot(snapshot):
    if snapshot.get("schemaVersion") != 1 or snapshot.get("source") != "verified-ci":
        raise ValueError("Expected a version 1 verified CI snapshot")
    if snapshot.get("dataset") != "synthetic e2e and load events":
        raise ValueError("Only the synthetic CI dataset can be published")
    if not re.fullmatch(r"[0-9a-f]{40}", snapshot.get("sourceCommit", "")):
        raise ValueError("Snapshot requires its exact source commit")
    if not re.fullmatch(
        r"https://github\.com/wxw2002a/pulseguard/actions/runs/[0-9]+",
        snapshot.get("runUrl", ""),
    ):
        raise ValueError("Snapshot must link to this repository's verification run")
    captured_at = datetime.fromisoformat(snapshot.get("capturedAt", "").replace("Z", "+00:00"))
    if captured_at.tzinfo is None:
        raise ValueError("Capture timestamp must include its timezone")
    for field in ("overview", "details", "evidence"):
        if not isinstance(snapshot.get(field), dict):
            raise ValueError(f"Snapshot {field} must be an object")
    for field in ("alerts", "transactions", "windows"):
        if not isinstance(snapshot.get(field), list):
            raise ValueError(f"Snapshot {field} must be an array")
    if snapshot["overview"].get("pendingDelivery") != 0:
        raise ValueError("Snapshot must be captured after the outbox drains")
    for alert in snapshot["alerts"]:
        alert_id = alert.get("id") or alert.get("_id")
        if not isinstance(snapshot["details"].get(alert_id), dict):
            raise ValueError(f"Missing captured detail for alert {alert_id}")
        if not isinstance(snapshot["evidence"].get(alert_id), list):
            raise ValueError(f"Missing captured evidence for alert {alert_id}")


def build(snapshot_path, output):
    snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
    validate_snapshot(snapshot)
    output = output.resolve()
    if output == ROOT or output == STATIC or STATIC in output.parents:
        raise ValueError("Output must be separate from the application source")
    output.mkdir(parents=True, exist_ok=True)
    for filename in ("styles.css", "app.js", "favicon.svg"):
        shutil.copyfile(STATIC / filename, output / filename)
    html = (STATIC / "index.html").read_text(encoding="utf-8")
    for filename in ("styles.css", "app.js", "favicon.svg"):
        html = html.replace(f'"/{filename}"', f'"./{filename}"')
    html = html.replace('href="/"', 'href="./"')
    html = html.replace(
        '<script src="./app.js" defer></script>',
        '<script src="./runtime.js"></script>\n    <script src="./app.js" defer></script>',
    )
    (output / "index.html").write_text(html, encoding="utf-8")
    (output / "runtime.js").write_text(
        'window.PULSEGUARD_RUNTIME = {mode: "snapshot", snapshotUrl: "./snapshot.json"};\n',
        encoding="utf-8",
    )
    (output / "snapshot.json").write_text(
        json.dumps(snapshot, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    (output / ".nojekyll").touch()
    print(f"Built GitHub Pages site at {output} from {snapshot['sourceCommit']}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", required=True, type=Path)
    parser.add_argument("--output", default=Path("_site"), type=Path)
    arguments = parser.parse_args()
    build(arguments.snapshot, arguments.output)
