# Retained verification artifacts

These files were copied from successful GitHub Actions artifacts, preserving the observed values.

| File | Source |
|---|---|
| `java-test-summary.json` | JUnit XML summaries from [verify 34742387094](https://github.com/wxw2002a/pulseguard/actions/runs/34742387094), commit `7554a95` |
| `e2e-report.json` | Actual Compose pipeline, replay, review, quarantine and Kafka-outage checks in the same run |
| `load-report.json` | 120-request, client-rate-limited HTTP acceptance exercise in the same run |
| `compose-status.txt` | Actual container state after the same run |
| `kubernetes-e2e-report.json` | [Kubernetes smoke 34742236882](https://github.com/wxw2002a/pulseguard/actions/runs/34742236882), commit `1897933` |
| `kubernetes-resources.txt` | Actual pods, PVCs, Services, HPA and PDB in that cluster |

The runtime screenshot is stored at `../assets/dashboard.png`. Full logs and original JUnit XML are available in the linked Actions artifacts while GitHub retains them. See [the verification record](../verification.md) for scope, timing and limitations.
