#!/bin/bash

# --- Cấu hình màu sắc ---
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${BLUE}🚀 BẮT ĐẦU CÀI ĐẶT HỆ THỐNG REAL-TIME FRAUD DETECTION...${NC}"

# 0. Khởi tạo Cluster (BẮT BUỘC CHẠY ĐẦU TIÊN)
echo -e "${GREEN}🖥️  Khởi động Minikube...${NC}"
# Bổ sung --driver=docker để đảm bảo tương thích trên Linux
minikube start --driver=docker --cpus=4 --memory=16000 --disk-size=50g

# 1. Tạo Namespaces (Dùng vòng lặp và Idempotent create)
echo -e "${GREEN}📦 Tạo Namespaces...${NC}"
NAMESPACES=("stream" "orchestration" "monitoring" "data-storage" "ml-model")
for ns in "${NAMESPACES[@]}"; do
  kubectl create namespace "$ns" --dry-run=client -o yaml | kubectl apply -f -
done

# 2. Cài đặt Helm Repositories
echo -e "${GREEN}📥 Cập nhật Helm Repositories...${NC}"
helm repo add strimzi https://strimzi.io/charts/
helm repo add apache-airflow https://airflow.apache.org/
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update

# 3. Cài đặt Kafka Operator (Strimzi)
echo -e "${GREEN}⚙️ Cài đặt Strimzi Kafka Operator...${NC}"
helm upgrade --install strimzi-operator strimzi/strimzi-kafka-operator --namespace stream

# 4. Cài đặt Database & Storage (Postgres & Redis)
echo -e "${GREEN}💾 Cài đặt PostgreSQL và Redis...${NC}"
# Thay thế `|| true` bằng `--dry-run` cho ConfigMap để đảm bảo đè an toàn khi cấu hình đổi
kubectl create configmap postgres-init-script \
  --from-file=create_db.sql=infra/postgres/create_db.sql \
  -n data-storage --dry-run=client -o yaml | kubectl apply -f -

kubectl apply -f infra/postgres/ -n data-storage
kubectl apply -f infra/redis/redis_service.yaml

# 5. Cài đặt Kafka Cluster
echo -e "${GREEN}🎡 Khởi tạo Kafka Cluster...${NC}"
kubectl apply -f infra/kafka/ -n stream
# Đã gỡ bỏ: `kubectl edit kafkanodepool` (Nếu cần sửa pool, hãy sửa trực tiếp trong file yaml ở infra/kafka/ trước khi apply)

# 6. Cài đặt Flink (JobManager & TaskManager)
echo -e "${GREEN}🌊 Cài đặt Apache Flink...${NC}"
kubectl create configmap flink-config \
  --from-file=flink-conf.yaml=./infra/flink/flink-config.yaml \
  --from-file=fraud_prediction.py=./flink_service/fraud_prediction.py \
  --from-file=init.sql=./flink_service/init.sql \
  -n stream --dry-run=client -o yaml | kubectl apply -f -

kubectl apply -f infra/flink/ -n stream

# 7. Cài đặt Monitoring (Prometheus & Grafana)
echo -e "${GREEN}📊 Cài đặt Monitoring Stack...${NC}"
helm upgrade --install monitor-stack prometheus-community/kube-prometheus-stack \
  -n monitoring \
  --set grafana.service.type=NodePort

# 8. Cài đặt Airflow
echo -e "${GREEN}🕰️ Cài đặt Apache Airflow...${NC}"
helm upgrade --install airflow apache-airflow/airflow \
  -f infra/airflow/override-values.yaml \
  -n orchestration

# RBAC Idempotent
kubectl create rolebinding airflow-worker-stream-admin \
  --clusterrole=admin \
  --serviceaccount=orchestration:airflow-worker \
  --namespace=stream --dry-run=client -o yaml | kubectl apply -f -

# 9. Cài đặt Data Generator
echo -e "${GREEN}🎲 Khởi động Data Generator...${NC}"
# Đã xóa lệnh tạo namespace stream thừa ở đây vì đã chạy ở bước 1
kubectl create configmap generator-script \
  --from-file=./infra/data-generator/data-generator.py \
  -n stream --dry-run=client -o yaml | kubectl apply -f -

kubectl apply -f ./infra/data-generator/generator-pod.yaml

# --- TỔNG KẾT ---
echo -e "${BLUE}✅ ĐÃ CÀI ĐẶT XONG! Hệ thống đang pull image và khởi tạo.${NC}"
echo -e "${YELLOW}🔗 TRUY CẬP NHANH:${NC}"
echo "- Flink UI:   http://$(minikube ip):30081"

# Xử lý lỗi an toàn nếu Grafana chưa kịp tạo Service NodePort lúc script chạy xong
GRAFANA_PORT=$(kubectl get svc -n monitoring monitor-stack-grafana -o jsonpath='{.spec.ports[0].nodePort}' 2>/dev/null || echo "<đang khởi tạo>")
echo "- Grafana:    http://$(minikube ip):${GRAFANA_PORT}"
echo "- Airflow:    kubectl port-forward svc/airflow-api-server 8080:8080 -n orchestration"