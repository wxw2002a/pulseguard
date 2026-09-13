#!/usr/bin/env bash
# Run from a machine with kubectl credentials and Spark 3.5.8/Java17 installed.
set -euo pipefail
: "${SPARK_HOME:?Set SPARK_HOME to a local Spark 3.5.8 installation}"
: "${SPARK_IMAGE:?Set SPARK_IMAGE to the built Dockerfile.streaming image accessible to every node}"
NAMESPACE="${NAMESPACE:-pulseguard}"
CHECKPOINT_PVC="${CHECKPOINT_PVC:-spark-checkpoints-rwx}"
K8S_API="${K8S_API:-$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}')}"
active_replicas="$(kubectl -n "$NAMESPACE" get deployment streaming -o jsonpath='{.spec.replicas}')"
if [[ "$active_replicas" != "0" ]]; then
  echo 'Scale the single-pod development processor to zero before native submission:' >&2
  echo "kubectl -n $NAMESPACE scale deployment/streaming --replicas=0" >&2
  exit 1
fi
claim_mode="$(kubectl -n "$NAMESPACE" get pvc "$CHECKPOINT_PVC" -o jsonpath='{.spec.accessModes[*]}')"
if [[ "$claim_mode" != *ReadWriteMany* ]]; then
  echo "Checkpoint PVC must support ReadWriteMany for driver and executor pods: $CHECKPOINT_PVC" >&2
  exit 1
fi
exec "$SPARK_HOME/bin/spark-submit" \
  --master "k8s://$K8S_API" \
  --deploy-mode cluster \
  --name pulseguard-streaming-native \
  --class io.pulseguard.streaming.PulseGuardStreaming \
  --conf "spark.kubernetes.namespace=$NAMESPACE" \
  --conf "spark.kubernetes.container.image=$SPARK_IMAGE" \
  --conf spark.kubernetes.container.image.pullPolicy=IfNotPresent \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-driver \
  --conf spark.executor.instances=2 \
  --conf spark.executor.cores=1 \
  --conf spark.executor.memory=1g \
  --conf spark.driver.memory=1g \
  --conf spark.kubernetes.memoryOverheadFactor=0.4 \
  --conf spark.sql.shuffle.partitions=6 \
  --conf "spark.kubernetes.driverEnv.KAFKA_BOOTSTRAP_SERVERS=kafka.$NAMESPACE.svc.cluster.local:9092" \
  --conf "spark.kubernetes.driverEnv.MONGODB_URI=mongodb://mongodb.$NAMESPACE.svc.cluster.local:27017/pulseguard" \
  --conf spark.kubernetes.driverEnv.CHECKPOINT_DIR=file:///checkpoints/native-v1 \
  --conf "spark.kubernetes.driver.volumes.persistentVolumeClaim.checkpoints.options.claimName=$CHECKPOINT_PVC" \
  --conf spark.kubernetes.driver.volumes.persistentVolumeClaim.checkpoints.mount.path=/checkpoints \
  --conf spark.kubernetes.driver.volumes.persistentVolumeClaim.checkpoints.mount.readOnly=false \
  --conf "spark.kubernetes.executor.volumes.persistentVolumeClaim.checkpoints.options.claimName=$CHECKPOINT_PVC" \
  --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.checkpoints.mount.path=/checkpoints \
  --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.checkpoints.mount.readOnly=false \
  local:///opt/pulseguard/pulseguard-streaming.jar
