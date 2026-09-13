# Kubernetes development deployment

The default deployment runs a real Kafka broker, MongoDB, the Java API, and Spark Structured Streaming on Kubernetes. Spark uses **one pod with `local[2]` execution**, a persistent checkpoint, and `Recreate` updates. This baseline does not claim distributed Spark execution or high availability. Kafka and MongoDB each have one replica and persistent storage.

Prerequisites: a running cluster, `kubectl`, a default dynamic storage class, and enough capacity (about 4 CPU and 6 GiB memory is a practical starting allocation). API autoscaling additionally needs metrics-server. Manifests have been designed for development; Kafka and MongoDB use plaintext internal connections without authentication.

Build the images and load them into your local cluster, or publish them to a registry and update the image references:

```bash
docker compose build api streaming
# For kind (skip this command for Docker Desktop's shared image store):
kind load docker-image pulseguard-api:local pulseguard-streaming:local
kubectl apply -f infra/k8s/base/namespace.yaml
kubectl -n pulseguard create secret generic pulseguard-secrets --from-literal=ingest-api-key=YOUR_RANDOM_DEVELOPMENT_KEY
kubectl apply -k infra/k8s/overlays/dev
kubectl -n pulseguard wait --for=condition=complete job/topic-init --timeout=300s
kubectl -n pulseguard rollout status deployment/api --timeout=300s
kubectl -n pulseguard rollout status deployment/streaming --timeout=300s
kubectl -n pulseguard port-forward service/api 8080:8080
```

In another terminal, set `INGEST_API_KEY` to the value you chose and run `python scripts/e2e.py`. The Compose-only `--with-recovery` switch should be omitted. The development overlay starts one API pod; the base starts two and provides a CPU HPA and a PDB. The development PDB preserves one available API pod and can therefore prevent node draining when only one replica exists. Scale to two before a drain. Spark's readiness probe only checks its UI socket; successful e2e processing is the stronger application-health check.

The base namespace and Kafka advertised hostname are deliberately fixed to `pulseguard`; changing namespaces also requires changing Kafka's advertised hostname and voter address. Keep checkpoint PVCs across ordinary restarts. Never run two copies of a query on one checkpoint directory. Deleting a checkpoint is a rebuild, not a harmless retry: existing absolute window snapshots may temporarily regress while history is replayed. Rebuild into fresh MongoDB collections/database and switch readers once caught up.

The manually triggered GitHub Actions workflow `kubernetes-smoke` builds the images, creates a one-node kind cluster, applies these development manifests, waits for the actual workloads, and executes the HTTP-to-Spark e2e checks. It uploads pod logs, Kubernetes events, and the measured test report. This workflow exercises the single-pod Spark development mode. It does not install metrics-server or test CPU-driven HPA scaling, and does not exercise native distributed Spark below.

## Native distributed Spark (optional)

`scripts/spark-submit-k8s.sh` uses Spark's actual Kubernetes cluster mode: the driver creates two executor pods using the included `spark-driver` service account. It requires a local **Spark 3.5.8 / Scala 2.12 / Java 17** installation and the streaming image accessible to every node. The Apache entrypoint is preserved in `Dockerfile.streaming` for driver/executor compatibility. Kafka connector jars are already inside the image.

A `ReadWriteMany` volume must be mounted at the same path on the driver and every executor, with UID/GID 185 write permission. Configure and apply `checkpoint-rwx.example.yaml` using a suitable CSI/NFS storage class, and wait until the claim is Bound. Local `ReadWriteOnce` storage is insufficient for this multi-node mode. The native path uses `file:///checkpoints/native-v1`, which refers to that same shared filesystem on all pods. For production prefer a durable filesystem/object store with the corresponding Hadoop connector and credentials configured; simply changing a URI to `s3a://` is insufficient.

```bash
kubectl -n pulseguard scale deployment/streaming --replicas=0
kubectl -n pulseguard wait --for=delete pod -l app=streaming --timeout=180s
export SPARK_HOME=/path/to/spark-3.5.8-bin-hadoop3
export SPARK_IMAGE=your-registry/pulseguard-streaming:1.0.0
bash scripts/spark-submit-k8s.sh
kubectl -n pulseguard get pods -l spark-role=executor
```

Use a fresh development MongoDB dataset for the first native-mode run, or explicitly plan a rebuild and reconcile results. The native script intentionally starts a distinct checkpoint rather than trying to reuse the single-pod claim. This optional mode is a deployable reference and must be validated on a cluster with shared storage; it is not exercised by the Compose CI job. Existing completed/failed native drivers should be cleaned up before submitting a replacement query; retain the shared checkpoint for recovery.

## Production gap

Before a production deployment, add Kafka replication and TLS/SASL, a MongoDB replica set with authentication/backups, private ingress with identity-based access, external secret management, network policies, resilient checkpoint storage, workload-specific capacity tests, and alerting on stalled stream progress. HPA scales the API only. Spark parallelism is set through executors and partitions, not by duplicating streaming deployments.

References: [Spark 3.5.8 on Kubernetes](https://spark.apache.org/docs/3.5.8/running-on-kubernetes.html), [Structured Streaming fault tolerance](https://spark.apache.org/docs/3.5.8/structured-streaming-programming-guide.html#fault-tolerance-semantics).
