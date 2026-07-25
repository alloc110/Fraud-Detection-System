#!/bin/bash

# --- Cấu hình màu sắc cho dễ nhìn ---
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

# --- BỔ SUNG CÁC HELM REPO CÒN THIẾU ---
echo -e "${GREEN}📥 Thêm các Helm Repository...${NC}"

# Repo cho Strimzi (Kafka Operator)
helm repo add strimzi https://strimzi.io/charts/

# Repo cho Apache Airflow
helm repo add apache-airflow https://airflow.apache.org/

# Repo cho Prometheus/Grafana (Cậu đã có, nhưng thêm vào cho chắc)
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts

# Update tất cả
helm repo update

echo -e "${BLUE}🚀 Bắt đầu quá trình cài đặt hệ thống Real-time Fraud Detection...${NC}"
# 0. Khởi động Minikube với cấu hình đủ mạnh để chạy tất cả các service
minikube start --cpus=4 --memory=11000

# 1. Tạo các Namespace cần thiết
echo -e "${GREEN}📦 Tạo Namespaces...${NC}"
kubectl create namespace stream || true
kubectl create namespace orchestration || true
kubectl create namespace monitoring || true
kubectl create namespace data-storage || true

# 2. Cài đặt Postgres & MinIO (Dữ liệu nền)
echo -e "${GREEN}💾 Cài đặt Database và Storage...${NC}"
kubectl apply -f infra/postgres/ -n data-storage
kubectl apply -f infra/minio/ -n data-storage


kubectl create configmap postgres-init-script \
  --from-file=create_db.sql=infra/postgres/create_db.sql \
  -n data-storage
# 3. Cài đặt Kafka (Dùng Strimzi Operator hoặc file YAML của Lộc)
helm upgrade --install strimzi-operator strimzi/strimzi-kafka-operator --namespace stream --create-namespace
helm repo update  


echo -e "${GREEN}🎡 Cài đặt Kafka Cluster...${NC}"
kubectl apply -f infra/kafka/ -n stream

# 4. Cài đặt Flink (JobManager & TaskManager)
echo -e "${GREEN}⚙️ Cài đặt Flink...${NC}"
# Tạo ConfigMap từ thư mục FlinkData trước khi chạy Deployment
kubectl delete configmap flink-config -n stream || true
kubectl create configmap flink-config \
  --from-file=flink-conf.yaml=infra/flink/flink-config.yaml \
  --from-file=fraud_model.json=flink_service/fraud_model.json \
  --from-file=fraud_prediction.py=flink_service/fraud_prediction.py \
  --from-file=init.sql=flink_service/init.sql \
  -n stream

kubectl apply -f infra/flink/ -n stream

# 5. Cài đặt Monitoring (Prometheus & Grafana)
echo -e "${GREEN}📊 Cài đặt Prometheus & Grafana...${NC}"
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update
helm install monitor-stack prometheus-community/kube-prometheus-stack -n monitoring --set grafana.service.type=NodePort

# 6. Cài đặt Airflow (Dùng bản v7 có kubectl)
echo -e "${GREEN}🌬️ Cài đặt Airflow...${NC}"
helm upgrade --install airflow apache-airflow/airflow -f infra/airflow/override-values.yaml -n orchestration

# 7. Cấp quyền RBAC "Thần thánh" cho Airflow
echo -e "${GREEN}🔑 Cấu hình quyền hạn RBAC cho Airflow...${NC}"
kubectl create rolebinding airflow-worker-stream-admin \
  --clusterrole=admin \
  --serviceaccount=orchestration:airflow-worker \
  --namespace=stream || true

echo "🚀 Đang triển khai Data Generator vào namespace 'stream'..."

# 1. Đảm bảo namespace stream đã tồn tại (nếu setup.sh chưa có)
kubectl create namespace stream --dry-run=client -o yaml | kubectl apply -f -

echo "📦 Đang nạp file data-generator.py vào ConfigMap..."
kubectl create configmap generator-script --from-file=./infra/data-generator/data-generator.py -n stream --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f ./infra/data-generator/generator-pod.yaml
echo "✅ Đã cài đặt xong Data Generator!"
echo "👉 Để xem log, hãy chạy: kubectl logs -f data-generator-pod -n stream"


echo -e "${BLUE}✅ ĐÃ CÀI ĐẶT XONG! Đợi các Pod chuyển sang trạng thái Running.${NC}"
echo -e "${BLUE}🔗 Truy cập nhanh:${NC}"
echo "- Flink UI: http://$(minikube ip):30081"
echo "- Grafana: http://$(minikube ip):$(kubectl get svc -n monitoring monitor-stack-grafana -o jsonpath='{.spec.ports[0].nodePort}')"
echo "- Airflow: Lộc dùng lệnh ' kubectl port-forward svc/airflow-api-server 8080:8080 --namespace orchestration' để vào UI"