#!/usr/bin/env python3
"""Validate checked-in JSON/XML/Python plus Compose and rendered Kustomize config."""
import ast
import json
from pathlib import Path
import shutil
import subprocess
import xml.etree.ElementTree as ET


def main():
    root = Path(__file__).resolve().parents[1]
    for folder in (root / "infra", root / "scripts"):
        for path in folder.rglob("*"):
            if path.suffix == ".json":
                json.loads(path.read_text(encoding="utf-8"))
            elif path.suffix == ".xml":
                ET.parse(path)
            elif path.suffix == ".py":
                ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for command in (["docker", "compose", "--profile", "monitoring", "config", "--quiet"],
                    ["kubectl", "kustomize", "infra/k8s/overlays/dev"]):
        if not shutil.which(command[0]):
            raise SystemExit(f"Required validation tool missing: {command[0]}")
        result = subprocess.run(command, cwd=root, text=True, capture_output=True)
        if result.returncode:
            raise SystemExit(result.stderr or result.stdout)
        print("PASS " + " ".join(command))
    print("PASS Python syntax, JSON dashboards, XML connector dependencies")


if __name__ == "__main__":
    main()
