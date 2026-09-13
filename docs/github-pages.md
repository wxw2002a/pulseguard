# Hosted workspace

Open **https://wxw2002a.github.io/pulseguard/**. The website source, build script and deployment workflow belong to this repository.

The site serves the same dashboard assets as the Spring Boot application. Its default mode reads a captured result from the real CI stack: Java accepts synthetic payment events, the durable outbox publishes to Kafka, Spark computes risk signals, and MongoDB stores the projections and review history. The timestamp and source verification run appear above the dashboard.

## Available interactions

- Navigate the overview, alert inbox, transaction ledger, account windows and pipeline architecture.
- Filter signals by severity and open their original transaction evidence and recorded review history.
- Turn on **Sample data** to try review decisions on separate illustrative records. These decisions remain in the browser tab and reset on refresh. Returning to the snapshot restores the captured results.

The default snapshot is read-only. It does not submit payments, save new reviews, run scenarios or represent a continuously running backend. GitHub Pages serves static files. Run [the local stack](../README.md#run-locally) for ingestion and streaming execution.

## Publication and provenance

1. The `verify` workflow starts Kafka, MongoDB, the Java API and Spark; executes pipeline, failure recovery, Java and browser checks; and drives synthetic load through the API.
2. The live dashboard capture waits for the durable outbox to drain. Its explicitly enabled CI export reads the actual overview, up to 200 alerts, transactions and windows, plus each alert's detail and up to 200 original evidence records. Default detail requests retain the latest 50 reviews and the total review count.
3. The export records `capturedAt`, the exact source commit and verification run URL. Local screenshot capture does not export a publishable snapshot by default.
4. After a successful same-repository `main` push verification, `publish-pages` checks out that exact source commit, downloads its `pipeline-evidence` artifact and builds the page using that snapshot. Failed checks do not replace the published site.
5. Official GitHub Pages actions upload and deploy the generated static directory. All assets use relative paths so the site works under `/pulseguard/`.

The capture is a point-in-time result from an eventually consistent system. Lists are bounded independently; overview totals can exceed the displayed record count. Source inputs are synthetic test events, not customer payments. A captured result demonstrates the verified run and is not a throughput benchmark or a claim of production traffic.

## Source files

| Component | Location |
|---|---|
| Shared dashboard | `services/api/src/main/resources/static/` |
| Actual API result export | `tests/dashboard/capture-live.cjs` |
| Static packaging and provenance validation | `scripts/build_pages.py` |
| Verification workflow | `.github/workflows/ci.yml` |
| GitHub Pages deployment | `.github/workflows/pages.yml` |
| Browser checks under a repository subpath | `tests/dashboard/pages-smoke.cjs` |

The browser suite checks captured data, source links, evidence and history, escaped operator text, read-only controls, sample isolation, mobile layout and absence of live API requests.

To inspect an exported artifact locally:

```bash
gh run download RUN_ID --repo wxw2002a/pulseguard --name pipeline-evidence --dir artifacts/pages-source
python scripts/build_pages.py --snapshot artifacts/pages-source/dashboard-snapshot.json --output _site
python -m http.server 8765 --directory _site
```

Open `http://localhost:8765`. Only use a verified synthetic CI export for publication. The `_site/` output is generated and ignored by Git.
