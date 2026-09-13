# Verified automated test results

Source commit: `7376d10c3360a690efec2298a2724b0b0165d53a`.

[Complete verification run](https://github.com/wxw2002a/pulseguard/actions/runs/34743432685) · [GitHub Pages and public browser run](https://github.com/wxw2002a/pulseguard/actions/runs/34743594340) · [Open the workspace](https://wxw2002a.github.io/pulseguard/)

All five verification jobs and **Quality gate** passed. The public deployment browser check passed as well. These files retain the observed synthetic-run results; later deployments can display a newer snapshot.

| Evidence | Retained file |
|---|---|
| Consolidated Actions report | [test-report.md](test-report.md) |
| 57 Java tests, no failures/errors/skips | [java-test-summary.json](java-test-summary.json) |
| Coverage measurements | [coverage-summary.json](coverage-summary.json), [API XML](api/jacoco.xml), [streaming XML](streaming/jacoco.xml) |
| Actual Compose fault/recovery checks | [e2e-report.json](e2e-report.json), [container state](compose-status.txt) |
| k6: 601 accepted events, 6/6 thresholds pass | [Raw metrics](k6-summary.json), [JUnit thresholds](k6-junit.xml) |
| Separate 120-request generator exercise | [load-report.json](load-report.json) |
| Actual kind Kubernetes pipeline | [Scenario report](kubernetes-e2e-report.json), [resource state](kubernetes-resources.txt) |
| Actual captured API results and provenance | [dashboard-snapshot.json](dashboard-snapshot.json) |
| Published website checks | [published-site-report.json](published-site-report.json) |

Coverage is measured only in the Maven Java test processes. k6 measures rate-limited HTTP acknowledgement behavior, not maximum capacity or end-to-end Spark latency. The Kubernetes scenario uses the single-node development configuration; its report marks recovery checks not performed there explicitly.

The consolidated report references original artifact paths and hashes. Full JUnit XML, coverage HTML, browser screenshots and service logs remain available in the linked Actions artifacts while GitHub retains them. The Java and coverage JSON summaries above were derived from that run's original XML; all other report files were copied without changing their recorded results.

Rebuild the retained hosted snapshot from the repository root:

```bash
python scripts/build_pages.py --snapshot docs/artifacts/automation-7376d10/dashboard-snapshot.json --output _site
python -m http.server 8765 --directory _site
```
