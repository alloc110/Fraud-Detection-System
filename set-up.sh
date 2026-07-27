#!/bin/bash
set -uo pipefail

# --- Cấu hình màu sắc ---
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

log()  { echo -e "${GREEN}$1${NC}"; }
info() { echo -e "${BLUE}$1${NC}"; }
warn() { echo -e "${YELLOW}$1${NC}"; }
die()  { echo -e "${RED}❌ $1${NC}"; exit 1; }

require_file() {
  [ -e "$1" ] || die "Không tìm thấy: $1 (đang ở thư mục $(pwd)?)"
}

info "🚀 BẮT ĐẦU (RE)DEPLOY HỆ THỐNG REAL-TIME FRAUD DETECTION..."

# 1. Tạo Namespaces
log "📦 Đảm bảo Namespaces tồn tại..."
NAMESPACES=("stream" "orchestration" "monitoring" "data-storage")
for ns in "${NAMESPACES[@]}"; do
  kubectl create namespace "$ns" --dry-run=client -o yaml | kubectl apply -f -
done

# 2. Cài đặt Helm Repositories
log "📥 Cập nhật Helm Repositories..."
helm repo add strimzi https://strimzi.io/charts/ 2>/dev/null || true
helm repo add apache-airflow https://airflow.apache.org/ 2>/dev/null || true
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts 2>/dev/null || true
helm repo update

# 3. Cài đặt Kafka Operator (Strimzi) & CHỜ ĐÚNG THỨ TỰ: operator Ready -> CRD established
log "⚙️ Cài đặt Strimzi Kafka Operator..."
helm upgrade --install strimzi-operator strimzi/strimzi-kafka-operator --namespace stream \
  || die "Helm install strimzi-operator thất bại"

warn "⏳ Đang đợi Strimzi Operator pod sẵn sàng (deployment rollout)..."
kubectl rollout status deployment/strimzi-cluster-operator -n stream --timeout=180s \
  || die "Strimzi operator không Ready sau 180s. Kiểm tra: kubectl get pods -n stream"

warn "⏳ Đang đợi Kubernetes đăng ký CRD của Strimzi..."
kubectl wait crd/kafkas.kafka.strimzi.io --for condition=established --timeout=120s \
  || die "CRD kafkas.kafka.strimzi.io chưa established. Kiểm tra: kubectl get crd | grep strimzi"
kubectl wait crd/kafkanodepools.kafka.strimzi.io --for condition=established --timeout=120s \
  || die "CRD kafkanodepools.kafka.strimzi.io chưa established"

# 4. Cài đặt Database & Storage (Postgres & Redis)
log "💾 Cài đặt PostgreSQL và Redis..."
require_file "infra/postgres/create_db.sql"
kubectl create configmap postgres-init-script \
  --from-file=create_db.sql=infra/postgres/create_db.sql \
  -n data-storage --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f infra/postgres/ -n data-storage
kubectl apply -f infra/redis/redis_service.yaml -n data-storage   # (sửa: thiếu -n ở bản gốc)

# 5. Cài đặt Kafka Cluster (Lúc này CRD đã sẵn sàng)
log "🎡 Khởi tạo Kafka Cluster..."
kubectl apply -f infra/kafka/ -n stream

warn "⏳ Đang đợi Kafka cluster Ready (có thể mất vài phút)..."
kubectl wait kafka --all --for=condition=Ready --timeout=300s -n stream 2>/dev/null \
  || warn "⚠️ Kafka chưa Ready sau 300s, tiếp tục nhưng Flink có thể chưa kết nối được ngay."

# 6. Cài đặt Flink (Sửa lỗi Annotation 256KB)
log "🌊 Cài đặt Apache Flink..."
require_file "infra/flink/flink-config.yaml"
require_file "flink_service/fraud_model.json"
require_file "flink_service/fraud_prediction.py"
require_file "flink_service/init.sql"

kubectl delete configmap flink-config -n stream --ignore-not-found
kubectl create configmap flink-config \
  --from-file=flink-conf.yaml=infra/flink/flink-config.yaml \
  --from-file=fraud_model.json=flink_service/fraud_model.json \
  --from-file=fraud_prediction.py=flink_service/fraud_prediction.py \
  --from-file=init.sql=flink_service/init.sql \
  -n stream

kubectl apply -f infra/flink/ -n stream

# 7. Cài đặt Monitoring (Prometheus & Grafana)
log "📊 Cài đặt Monitoring Stack..."
helm upgrade --install monitor-stack prometheus-community/kube-prometheus-stack \
  -n monitoring \
  --set grafana.service.type=NodePort

# 8. Cài đặt Airflow
log "🕰️ Cài đặt Apache Airflow..."
require_file "infra/airflow/override-values.yaml"
helm upgrade --install airflow apache-airflow/airflow \
  -f infra/airflow/override-values.yaml \
  -n orchestration

kubectl create rolebinding airflow-worker-stream-admin \
  --clusterrole=admin \
  --serviceaccount=orchestration:airflow-worker \
  --namespace=stream --dry-run=client -o yaml | kubectl apply -f -

# 9. Cài đặt Data Generator
log "🎲 Khởi động Data Generator..."
require_file "infra/data-generator/data-generator.py"
kubectl delete configmap generator-script -n stream --ignore-not-found
kubectl create configmap generator-script \
  --from-file=./infra/data-generator/data-generator.py \
  -n stream
kubectl apply -f ./infra/data-generator/generator-pod.yaml

info "✅ ĐÃ DEPLOY XONG!"