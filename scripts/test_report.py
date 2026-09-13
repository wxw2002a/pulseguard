#!/usr/bin/env python3
"""Summarize downloaded PulseGuard CI artifacts without replacing upstream job results.

Exit 0: required evidence is present and the observed checks passed.
Exit 1: missing required evidence, malformed reports, or observed failures/errors.
--allow-incomplete tolerates missing evidence only; it never suppresses a failure.
Only TEST-*.xml contributes JUnit counts. Failsafe summaries are inspected solely
for discovery/launcher errors, avoiding integration-test double counting.
Required evidence: JUnit, Compose e2e, Kubernetes e2e, HTTP load, k6 thresholds,
and JaCoCo reports for both Java modules. Dashboard logs are optional.
"""

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sys
import xml.etree.ElementTree as ET


CHECKS = {
    "httpReplayChecked": "Exact HTTP replay is idempotent",
    "conflictChecked": "Conflicting transaction IDs are rejected",
    "originalEvidenceChecked": "Original transaction evidence is linked",
    "checkpointRecoveryChecked": "Spark checkpoint recovery processes new events",
    "analystReviewPreservedOnReplay": "Replay preserves analyst review and audit history",
    "deadLetterIdentityChecked": "Malformed input retains a deterministic dead-letter identity",
    "brokerOutageRecoveryChecked": "The durable outbox recovers after a Kafka outage",
}
BASE_CHECKS = {"httpReplayChecked", "conflictChecked", "originalEvidenceChecked"}
RULES = {"HIGH_VALUE", "VELOCITY", "CARD_TESTING"}
ANSI = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")


def markdown(value):
    """Keep artifact-provided values literal in Markdown tables and summaries."""
    return (str(value).replace("&", "&amp;").replace("<", "&lt;")
            .replace(">", "&gt;").replace("|", "&#124;").replace("`", "&#96;")
            .replace("\r", " ").replace("\n", " "))


def integer(value, label):
    if isinstance(value, bool) or not re.fullmatch(r"[0-9]+", str(value)):
        raise ValueError(f"{label} must be a nonnegative integer")
    return int(value)


def number(value, label):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
        raise ValueError(f"{label} must be a finite nonnegative number")
    return value


def tag(element):
    return element.tag.rsplit("}", 1)[-1]


@dataclass
class Suite:
    name: str
    phase: str
    module: str
    tests: int
    failures: int
    errors: int
    skipped: int
    source: Path

    @property
    def passed(self):
        return self.tests - self.failures - self.errors - self.skipped


class Report:
    def __init__(self, directory, allow_incomplete):
        self.directory = directory.resolve()
        self.allow_incomplete = allow_incomplete
        self.issues = []
        self.sources = {}
        self.sections = []

    def relative(self, path):
        return path.relative_to(self.directory).as_posix()

    def issue(self, message, missing=False):
        self.issues.append(("MISSING" if missing else "FAILED", message))

    def source(self, path):
        content = path.read_bytes()
        self.sources[self.relative(path)] = hashlib.sha256(content).hexdigest()
        return content

    def files(self, name):
        return sorted(path for path in self.directory.rglob(name) if path.is_file())

    def json_file(self, name, required=True):
        candidates = self.files(name)
        if not candidates:
            if required:
                self.issue(f"Required report {name} is missing.", missing=True)
            return None, None
        if len(candidates) != 1:
            self.issue(f"Found {len(candidates)} copies of {name}; use a fresh directory containing artifacts from one run.")
            for path in candidates:
                self.source(path)
            return None, None
        path = candidates[0]
        try:
            value = json.loads(self.source(path).decode("utf-8-sig"))
            if not isinstance(value, dict):
                raise ValueError("report must be a JSON object")
            return path, value
        except (OSError, UnicodeError, ValueError) as error:
            self.issue(f"Invalid {self.relative(path)}: {error}")
            return path, None

    def java(self):
        suites, seen = [], {}
        paths = self.files("TEST-*.xml")
        if not paths:
            self.issue("Required JUnit TEST-*.xml reports are missing.", missing=True)
        for path in paths:
            try:
                content = self.source(path)
                root = ET.fromstring(content)
                elements = [element for element in root.iter() if tag(element) == "testsuite"]
                if not elements:
                    raise ValueError("no testsuite element")
                parts = path.relative_to(self.directory).parts
                module = next((parts[index - 1] for index in range(len(parts) - 1, 0, -1)
                               if parts[index] == "target"), "unknown")
                phase = "integration" if "failsafe-reports" in parts else "unit / Spark"
                for element in elements:
                    # Parent testsuites can contain aggregate counts: count only leaf suites.
                    if any(tag(child) == "testsuite" for child in element):
                        continue
                    counts = {key: integer(element.get(key, "0"), key)
                              for key in ("tests", "failures", "errors", "skipped")}
                    if counts["tests"] < sum(counts[key] for key in ("failures", "errors", "skipped")):
                        raise ValueError("JUnit outcomes exceed the declared test count")
                    cases = [child for child in element if tag(child) == "testcase"]
                    if cases and len(cases) != counts["tests"]:
                        raise ValueError("JUnit testcase count disagrees with the suite total")
                    if cases:
                        for counter, outcome in (("failures", "failure"), ("errors", "error"), ("skipped", "skipped")):
                            observed = sum(any(tag(child) == outcome for child in case) for case in cases)
                            if observed != counts[counter]:
                                raise ValueError(f"JUnit {counter} disagree with testcase outcomes")
                    name = element.get("name", path.stem)
                    identity = (module, phase, name)
                    signature = ET.tostring(element)
                    if identity in seen:
                        if seen[identity] != signature:
                            raise ValueError(f"conflicting duplicate suite {name}; artifacts may contain multiple runs")
                        continue
                    seen[identity] = signature
                    suite = Suite(name, phase, module, source=path, **counts)
                    suites.append(suite)
                    if suite.failures or suite.errors:
                        self.issue(f"JUnit {name}: {suite.failures} failures, {suite.errors} errors.")
            except (OSError, ET.ParseError, ValueError) as error:
                self.issue(f"Invalid {self.relative(path)}: {error}")
        for path in self.files("failsafe-summary.xml"):
            try:
                root = ET.fromstring(self.source(path))
                fields = {tag(child): (child.text or "").strip() for child in root}
                errors = integer(fields.get("errors", "0"), "Failsafe errors")
                failures = integer(fields.get("failures", "0"), "Failsafe failures")
                if errors or failures or root.get("timeout") == "true" or fields.get("failureMessage"):
                    self.issue(f"Failsafe recorded a failure, timeout, or launcher error in {self.relative(path)}; inspect the original report.")
            except (OSError, ET.ParseError, ValueError) as error:
                self.issue(f"Invalid {self.relative(path)}: {error}")
        lines = ["## Java and Spark tests", ""]
        if not suites:
            lines.append("No readable JUnit test suites were found; no Java pass is claimed.")
        else:
            totals = {key: sum(getattr(suite, key) for suite in suites)
                      for key in ("tests", "passed", "failures", "errors", "skipped")}
            if totals["tests"] == 0:
                self.issue("JUnit reports contain zero executed test cases.", missing=True)
            lines += [f"Observed **{totals['tests']} test cases: {totals['passed']} passed, "
                      f"{totals['failures']} failed, {totals['errors']} errors, {totals['skipped']} skipped**.", "",
                      "| Suite | Module / phase | Passed | Failed | Errors | Skipped |",
                      "|---|---|---:|---:|---:|---:|"]
            for suite in suites:
                lines.append(f"| {markdown(suite.name)} | {markdown(suite.module)} / {suite.phase} | "
                             f"{suite.passed} | {suite.failures} | {suite.errors} | {suite.skipped} |")
            lines += ["", "JUnit suite files are counted once; Failsafe summaries are not added to those totals. "
                      "Skipped tests are shown explicitly and are not counted as passes."]
        self.sections.append("\n".join(lines))

    def e2e(self, kubernetes=False):
        filename = "kubernetes-e2e-report.json" if kubernetes else "e2e-report.json"
        title = "Kubernetes smoke test" if kubernetes else "Compose pipeline and recovery"
        path, data = self.json_file(filename)
        lines = [f"## {title}", ""]
        if data is None:
            lines.append("Required report is missing or unreadable; no pass is claimed.")
            self.sections.append("\n".join(lines))
            return
        lines += [f"Reported status: **{markdown(data.get('status', 'not recorded'))}**. "
                  f"Scenario run ID: `{markdown(data.get('runId', 'not recorded'))}`. "
                  f"Completed: {markdown(data.get('completedAt', 'not recorded'))}.", "",
                  "| Check | Observed result |", "|---|---|"]
        if data.get("status") != "passed":
            self.issue(f"{filename} did not report status=passed.")
        required_checks = BASE_CHECKS if kubernetes else set(CHECKS)
        for field, label in CHECKS.items():
            value = data.get(field)
            outcome = "PASS" if value is True else "NOT CHECKED" if value is False else "NOT RECORDED"
            lines.append(f"| {label} | {outcome} |")
            if field in required_checks and value is not True:
                self.issue(f"{filename} does not establish required check {field}.")
        observed = data.get("rulesObserved", [])
        if not isinstance(observed, list) or any(not isinstance(rule, str) for rule in observed):
            self.issue(f"{filename} rulesObserved must be a list of strings.")
            observed = []
        if not RULES.issubset(set(observed)):
            self.issue(f"{filename} is missing one or more required risk rules.")
        lines += ["", f"Rules observed: {', '.join(markdown(rule) for rule in observed) or 'none recorded'}."]
        for field, label in (("acceptedUniqueEvents", "Accepted unique scenario events"),
                             ("windowTransactionCount", "Checked window: unique transactions"),
                             ("windowTotalAmountMinor", "Checked window: amount in minor units"),
                             ("elapsedSeconds", "Scenario elapsed seconds")):
            try:
                value = number(data.get(field), field)
                lines.append(f"- {label}: **{value}**.")
            except ValueError as error:
                self.issue(f"Invalid {filename}: {error}")
        if data.get("windowTransactionCount") != 6 or data.get("windowTotalAmountMinor") != 3000:
            self.issue(f"{filename} did not preserve the six-event / 3000-minor-unit window fixture.")
        lines += ["", "These are recorded scenario checks, not production capacity or fraud-accuracy measurements."]
        if kubernetes:
            lines.append("Kubernetes evidence covers the one-node development stack. Recovery checks marked NOT CHECKED "
                         "were not performed in that scenario; this does not prove multi-node Spark, HA, or HPA scaling.")
        self.sections.append("\n".join(lines))

    def load(self):
        path, data = self.json_file("load-report.json")
        lines = ["## Synthetic HTTP acceptance exercise", ""]
        if data is None:
            lines.append("Required load report is missing or unreadable; no performance result is claimed.")
            self.sections.append("\n".join(lines))
            return
        lines += ["The CI exercise is **client-rate-limited**. Observed HTTP acknowledgement rates are "
                  "**not maximum throughput**, and request latency is **not end-to-end Spark latency**. "
                  "The current source report does not record the configured target rate.", "",
                  f"Measured: {markdown(data.get('measuredAt', 'not recorded'))}. "
                  f"Scenario run ID: `{markdown(data.get('runId', 'not recorded'))}`.", "",
                  "| Measurement | Observed value |", "|---|---:|"]
        try:
            submitted = integer(data.get("submitted"), "submitted")
            accepted = integer(data.get("acceptedResponses"), "acceptedResponses")
            duplicates = integer(data.get("duplicateResponses"), "duplicateResponses")
            if submitted < 1 or accepted > submitted or duplicates > accepted:
                raise ValueError("inconsistent submitted/accepted/duplicate counts")
            statuses = data.get("statusCounts")
            if not isinstance(statuses, dict):
                raise ValueError("statusCounts must be an object")
            counts = {str(status): integer(count, "HTTP status count") for status, count in statuses.items()}
            if sum(counts.values()) != submitted or counts.get("202", 0) != accepted:
                raise ValueError("HTTP status counts disagree with submitted/accepted counts")
            failures = data.get("failures")
            if not isinstance(failures, list):
                raise ValueError("failures must be an array")
            if accepted != submitted or failures:
                self.issue("The HTTP acceptance exercise contains rejected or failed requests.")
            metrics = [("Submitted requests", submitted), ("HTTP 202 responses", accepted),
                       ("Duplicate acknowledgements", duplicates),
                       ("Elapsed seconds", number(data.get("elapsedSeconds"), "elapsedSeconds")),
                       ("Accepted responses / second", number(data.get("acceptedResponsesPerSecond"), "acceptedResponsesPerSecond"))]
            latency = data.get("requestLatencyMs")
            if not isinstance(latency, dict):
                raise ValueError("requestLatencyMs must be an object")
            metrics += [(f"HTTP request latency {key} (ms)", number(latency.get(key), key))
                        for key in ("p50", "p95", "p99", "max")]
            for label, value in metrics:
                lines.append(f"| {label} | {value} |")
            lines += ["", "HTTP statuses: " + ", ".join(f"{markdown(status)} = {count}" for status, count in counts.items()) + "."]
        except ValueError as error:
            self.issue(f"Invalid load-report.json: {error}")
            lines += ["", "Some measurements could not be validated; inspect the source report."]
        self.sections.append("\n".join(lines))

    def dashboard(self):
        paths = self.files("dashboard*.log")
        lines = ["## Dashboard checks", ""]
        if not paths:
            lines.append("Optional dashboard log was not supplied. No browser-test result is inferred from other artifacts.")
        else:
            markers = []
            for path in paths:
                content = ANSI.sub("", self.source(path).decode("utf-8", errors="replace"))
                for line in content.splitlines():
                    if re.match(r"^PASS(?::|\s)", line):
                        markers.append((self.relative(path), line[:600]))
            if markers:
                lines.append(f"Observed {len(markers)} explicit success markers (not a JUnit test-case count):")
                lines.append("")
                lines.extend(f"- {markdown(line)} — `{markdown(source)}`" for source, line in markers)
            else:
                lines.append("Dashboard logs contain no explicit PASS markers; no browser pass is claimed.")
            lines += ["", "A success marker does not establish the final process exit status; the upstream dashboard job remains authoritative."]
        self.sections.append("\n".join(lines))

    def k6(self):
        _, data = self.json_file("k6-summary.json")
        lines = ["## k6 performance regression", ""]
        if data is None:
            lines.append("Required k6 report is missing or unreadable; no threshold pass is claimed.")
            self.sections.append("\n".join(lines))
            return
        try:
            if data.get("tool") != "k6":
                raise ValueError("tool must be k6")
            criteria = data.get("criteria")
            if not isinstance(criteria, list) or not criteria:
                raise ValueError("criteria must be a nonempty array")
            for criterion in criteria:
                if (not isinstance(criterion, dict) or not isinstance(criterion.get("name"), str)
                        or not isinstance(criterion.get("passed"), bool)):
                    raise ValueError("each criterion requires a name and boolean passed result")
            passed = sum(criterion["passed"] for criterion in criteria)
            if passed != len(criteria):
                self.issue(f"k6: {len(criteria) - passed} of {len(criteria)} regression thresholds failed.")
            rate = number(data.get("configuredRate"), "configuredRate")
            duration = number(data.get("durationSeconds"), "durationSeconds")
            expected = integer(data.get("expectedAcceptedEvents"), "expectedAcceptedEvents")
            if rate <= 0 or duration <= 0 or expected != rate * duration:
                raise ValueError("configured rate, duration and expected event count disagree")
            results = data.get("results")
            if not isinstance(results, dict) or not isinstance(results.get("metrics"), dict):
                raise ValueError("results.metrics must be an object")
            metrics = results["metrics"]
            accepted = number(metrics.get("accepted_events", {}).get("values", {}).get("count"), "accepted_events count")
            latency = metrics.get("http_req_duration{endpoint:ingest}", {}).get("values", {})
            p95 = number(latency.get("p(95)"), "ingestion p95")
            p99 = number(latency.get("p(99)"), "ingestion p99")
            if accepted < expected:
                self.issue(f"k6 accepted {accepted} events, below its declared target of {expected}.")
            lines += [f"**{passed}/{len(criteria)} thresholds passed.** "
                      f"Configured load: **{rate} events/s for {duration}s**; "
                      f"**{accepted}** accepted events, target **{expected}**.", "",
                      f"HTTP ingestion request latency: p95 **{p95:.2f} ms**, p99 **{p99:.2f} ms**.", "",
                      "This is a rate-limited HTTP regression test, not a maximum-throughput measurement "
                      "or an end-to-end Spark latency result.", "",
                      f"Recorded scope: {markdown(data.get('scope', 'not recorded'))}.", "",
                      f"Recorded source commit: `{markdown(data.get('sourceCommit') or 'not recorded')}`.", "",
                      "| k6 criterion | Result |", "|---|---|"]
            lines.extend(f"| {markdown(criterion['name'])} | {'PASS' if criterion['passed'] else 'FAIL'} |"
                         for criterion in criteria)
        except (ValueError, AttributeError) as error:
            self.issue(f"Invalid k6-summary.json: {error}")
            lines.append("Some k6 measurements or threshold outcomes could not be validated.")
        self.sections.append("\n".join(lines))

    def coverage(self):
        paths = self.files("jacoco.xml")
        modules = set()
        lines = ["## JaCoCo coverage", ""]
        if not paths:
            lines.append("Required JaCoCo XML reports were not supplied. No coverage percentage is inferred.")
        else:
            lines += ["Coverage is measured from the attached execution data. No arbitrary minimum percentage "
                      "is enforced; unexecuted production code remains in the denominator.", "",
                      "| Report | Covered lines | Missed lines | Line coverage |", "|---|---:|---:|---:|"]
            for path in paths:
                try:
                    root = ET.fromstring(self.source(path))
                    # Only the report-level counter; package/class/source counters repeat the same lines.
                    counters = [child for child in root if tag(child) == "counter" and child.get("type") == "LINE"]
                    if tag(root) != "report" or len(counters) != 1:
                        raise ValueError("expected one report-level LINE counter")
                    covered = integer(counters[0].get("covered"), "covered lines")
                    missed = integer(counters[0].get("missed"), "missed lines")
                    total = covered + missed
                    if total == 0:
                        raise ValueError("coverage report contains no executable source lines")
                    packages = [child.get("name", "") for child in root if tag(child) == "package"]
                    for module in ("api", "streaming"):
                        if any(package.startswith(f"io/pulseguard/{module}") for package in packages):
                            if module in modules:
                                raise ValueError(f"duplicate coverage report for module {module}")
                            modules.add(module)
                    percentage = f"{100 * covered / total:.2f}%" if total else "N/A (no executable lines)"
                    lines.append(f"| {markdown(self.relative(path))} | {covered} | {missed} | {percentage} |")
                except (OSError, ET.ParseError, ValueError) as error:
                    self.issue(f"Invalid {self.relative(path)}: {error}")
        for module in ("api", "streaming"):
            if module not in modules:
                self.issue(f"Required {module} JaCoCo report with executable application lines is missing.", missing=True)
        self.sections.append("\n".join(lines))

    def provenance(self):
        lines = ["## Artifact provenance", "",
                 "Counts and assertions above are derived from the supplied files. A scenario run ID inside an e2e/load "
                 "report is a generated fixture identifier, not a GitHub Actions run ID."]
        repository = os.environ.get("GITHUB_REPOSITORY", "")
        run_id = os.environ.get("GITHUB_RUN_ID", "")
        commit = os.environ.get("GITHUB_SHA", "")
        if re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository) and re.fullmatch(r"[0-9]+", run_id):
            lines += ["", f"Report-generation workflow: [GitHub Actions run {run_id}](https://github.com/{repository}/actions/runs/{run_id})."]
        if re.fullmatch(r"[0-9a-fA-F]{40}", commit):
            lines += ["", f"Report-generation commit: `{commit}`."]
        _, snapshot = self.json_file("dashboard-snapshot.json", required=False)
        if snapshot:
            lines += ["", "Embedded dashboard snapshot metadata: "
                      f"source commit `{markdown(snapshot.get('sourceCommit', 'not recorded'))}`, "
                      f"run `{markdown(snapshot.get('runUrl', 'not recorded'))}`, "
                      f"captured {markdown(snapshot.get('capturedAt', 'not recorded'))}."]
        lines += ["", "Workflow context alone does not authenticate an arbitrary local download. "
                  "Download artifacts from one intended workflow run into an empty directory and retain the source run URL. "
                  "SHA-256 values identify the exact report bytes used here.", "",
                  "| Source path relative to artifact directory | SHA-256 |", "|---|---|"]
        for path, digest in sorted(self.sources.items()):
            lines.append(f"| {markdown(path)} | `{digest}` |")
        self.sections.append("\n".join(lines))

    def write(self, output):
        self.java()
        self.e2e()
        self.e2e(kubernetes=True)
        self.load()
        self.k6()
        self.dashboard()
        self.coverage()
        self.provenance()
        failures = [message for kind, message in self.issues if kind == "FAILED"]
        missing = [message for kind, message in self.issues if kind == "MISSING"]
        status = "FAILED" if failures else "INCOMPLETE" if missing else "PASSED"
        intro = ["# PulseGuard automated test report", "",
                 f"Artifact assessment: **{status}**. Generated {datetime.now(timezone.utc).isoformat()}.", "",
                 "This report summarizes observed artifact evidence. The final quality gate must also require "
                 "successful upstream jobs; report generation cannot turn a failed or cancelled job into a pass."]
        if self.issues:
            intro += ["", "## Evidence issues", ""]
            intro.extend(f"- **{kind}**: {markdown(message)}" for kind, message in self.issues)
        if missing and self.allow_incomplete:
            intro += ["", "`--allow-incomplete` tolerates missing files for diagnostics; it does not establish a complete pass."]
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text("\n".join(intro) + "\n\n" + "\n\n".join(self.sections) + "\n", encoding="utf-8")
        print(f"{status}: wrote {output}; {len(failures)} failure issue(s), {len(missing)} missing report(s)")
        return 1 if failures or (missing and not self.allow_incomplete) else 0


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--artifacts", required=True, type=Path,
                        help="Fresh directory of downloaded artifacts from one run; searched recursively")
    parser.add_argument("--output", required=True, type=Path, help="English Markdown report path (written even on failure)")
    parser.add_argument("--allow-incomplete", action="store_true",
                        help="Allow missing required reports for diagnostics; malformed reports and observed failures still exit 1")
    arguments = parser.parse_args(argv)
    if arguments.artifacts.exists() and not arguments.artifacts.is_dir():
        parser.error("--artifacts must be a directory")
    return Report(arguments.artifacts, arguments.allow_incomplete).write(arguments.output)


if __name__ == "__main__":
    sys.exit(main())
