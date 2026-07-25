# 📊 Real-time Data-driven Commercial Transaction Identification V2 (Cloud☁️☁️☁️)

![Python](https://img.shields.io/badge/Python-3.13+-blue?style=for-the-badge&logo=python&logoColor=white)
![Apache Flink](https://img.shields.io/badge/Apache%20Flink-1.17-E6522C?style=for-the-badge&logo=apache-flink&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-3.5-black?style=for-the-badge&logo=apache-kafka&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-3.0-017CEE?style=for-the-badge&logo=apache-airflow&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-0.95-009688?style=for-the-badge&logo=fastapi&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)
![Kubernetes](https://img.shields.io/badge/Kubernetes-1.27-326ce5?style=for-the-badge&logo=kubernetes&logoColor=white)
![Prometheus](https://img.shields.io/badge/Prometheus-E6522C?style=for-the-badge&logo=prometheus&logoColor=white)
![Grafana](https://img.shields.io/badge/Grafana-F46800?style=for-the-badge&logo=grafana&logoColor=white)

![Banner Alloc](assets/images/my_banner.png)

> **Project:** Real-time Architectural Analysis and Fraud Prediction System  
> **University:** Ho Chi Minh City Open University (HCMOU)  
> **Tech Stack:** PyFlink, Apache Kafka, Apache Airflow, PostgreSQL, Prometheus & Grafana

J-DataPipe is a comprehensive Data Platform that seamlessly integrates real-time stream processing with advanced predictive analytics. The system leverages an integrated XGBoost model to provide high-accuracy value forecasting and anomaly detection.
This is V2.

---

## 📋 Table of Contents

- [Repository Structure](#-repository-structure)
- [High-level System Architecture](#-high-level-system-architecture)
- [Prerequisites](#-prerequisites)
- [Installation & Setup](#-installation--setup)
- [Running the Application](#-running-the-application)
- [Monitoring & Observability](#-monitoring--observability)
- [Data Flow Pipeline](#-data-flow-pipeline)
- [Demo Video](#-demo-video)

---

## 📂 Repository Structure

```bash
.
.
├── assets/                         # 🎨 Tài nguyên đa phương tiện
│   ├── images/
│   │   └── architecture.png        # Sơ đồ kiến trúc tổng thể của hệ thống
│   └── videos/                     # Video demo vận hành (FastAPI, Grafana)
│
├── dags/                           # 🌬️ Apache Airflow Orchestration
│   ├── fake_transaction.py         # DAG tự động tạo & bơm dữ liệu giả vào Kafka
│   ├── init_data.py                # DAG khởi tạo dữ liệu mẫu cho Postgres/MinIO
│   └── start_flink.py              # DAG điều khiển và giám sát trạng thái Flink Job
│
├── flind_service/                      # ⚙️ Real-time Processing (PyFlink)
│   ├── fraud_model.json            # Trọng số mô hình XGBoost đã được train
│   ├── fraud_prediction.py         # Script xử lý dòng (Stream) & dự báo gian lận
│   └── init.sql                    # Định nghĩa bảng (DDL) cho Flink SQL Connector
│
├── infra/                          # 🏗️ Infrastructure as Code (Kubernetes)
│   ├── airflow/
│   │   └── override-values.yaml# Cấu hình tùy chỉnh cho Helm Chart Airflow
│   ├── flink/                      # Cấu hình cụm Flink (JobManager, TaskManager)
│   │   ├── deployment.yaml
│   │   ├── flink-config.yaml       # Biến môi trường và cấu hình vận hành Flink
│   │   ├── taskmanager.yaml        # Định nghĩa tài nguyên xử lý của TaskManager
│   │   └── ui-jobmanager.yaml      # Service mở cổng Web UI cho Flink (30081)
│   ├── kafka/                      # Hệ thống truyền tin (Strimzi/Kafka-on-K8s)
│   │   ├── connect.yaml            # 🔌 Cấu hình Kafka Connect Cluster - Nền tảng để chạy các bộ kết nối (Connectors)
│   │   ├── kafka-kraft.yaml        # 🚀 Cấu hình Kafka KRaft mode
│   │   ├── kafka.yaml              # 🛠️ Định nghĩa Cluster Kafka
│   │   ├── nodepool.yaml           # 🏗️ Quản lý nhóm các Node trong Kafka, giúp tách biệt vai trò Controller và Broker
│   │   └── postgres-connect.yaml   # 📥 Sink Connector: Tự động đẩy dữ liệu từ Kafka Topic vào PostgreSQL (Database)
│   └── postgres/                   # Database lưu trữ kết quả cuối cùng
│       └── postgres-hive-db.yaml
│
├── notebooks/                         # 🧠 Machine Learning Development
│   └── training.py                 # Script huấn luyện mô hình XGBoost từ dữ liệu thô
│
├── scripts/                        # 🛠️ Database & Automation Scripts
│   └── create_db.sql               # Khởi tạo Schema và bảng cho PostgreSQL
│
├── script.sh                       # 🚀 Master Script: Cài đặt toàn bộ hệ thống 1-click
├── Dockerfile                      # Đóng gói môi trường thực thi (v7, v8...)
├── README.md                       # Tài liệu hướng dẫn chi tiết
```

---

## 🏗 High-level System Architecture

![High-level System Architecture](assets/images/image.png)

### 1. Orchestration & Ingestion

- **Apache Airflow:** Orchestrates data pipelines, managing data flow from sources (Data Generation) to centralized storage systems.

### 2. Hybrid Data Storage

- **Data Warehouse (PostgreSQL):** Stores structured data to support rapid analytical queries and business intelligence.

### 3. Real-time Streaming

- **Debezium (CDC):** Implements Change Data Capture to track and extract real-time data modifications from PostgreSQL.
- **Apache Kafka:** A high-throughput message queuing system that decouples and coordinates data streams between services.
- **Apache Flink:** Performs low-latency, stateful stream processing for real-time analytics and transformations.

### 4. AI & Prediction Layer

- **XGBoost:** Executes predictive modeling and real-time inference based on processed data features delivered by the pipeline.

### 5. Monitoring & Observability

- **Prometheus:** Collects performance metrics across the entire Kubernetes infrastructure in real-time.
- **Grafana:** Provides comprehensive visualization of system performance through interactive, real-time dashboards.

---

# 🔧 Prerequisites

To run the **J-DataPipe** project locally, ensure your machine meets the hardware requirements and has the following tools installed:

### 🖥️ Hardware Recommendations

Because the pipeline runs multiple heavy-duty services (Kafka, Flink, Airflow, and Monitoring), your system should ideally have:

- **CPU:** 4+ Cores
- **RAM:** 16GB (Minikube requires at least **8GB** dedicated to the cluster)
- **Disk:** 20GB free space

### 🛠️ Required Tools

| Tool               | Description                                                                      | Download / Guide                                                 |
| :----------------- | :------------------------------------------------------------------------------- | :--------------------------------------------------------------- |
| **Docker Desktop** | Essential container runtime to build images and host the Minikube node.          | [Download Here](https://www.docker.com/products/docker-desktop/) |
| **Minikube**       | Local Kubernetes cluster orchestrator.                                           | [Installation Guide](https://minikube.sigs.k8s.io/docs/start/)   |
| **Kubectl**        | The standard CLI tool for interacting with the Kubernetes API.                   | [Install Kubectl](https://kubernetes.io/docs/tasks/tools/)       |
| **Helm v3**        | K8s Package Manager used for deploying Airflow and the Prometheus/Grafana stack. | [Install Helm](https://helm.sh/docs/intro/install/)              |
| **Python 3.13+**   | Required for local ML training (`models/`) and PyFlink development.              | [Download Python](https://www.python.org/downloads/)             |

---

### ✅ Verification

Run the following commands in your terminal to verify the installation:

```bash
docker --version      # Should be v20.10+
minikube version      # Should be v1.30+
kubectl version --client
helm version
python --version
```

---

## 🕹️ Installation & Setup

```bash
bash script.sh
```

kubectl exec -it airflow-api-server-547b89fc49-l2h4r -n orchestration -- airflow users create \

>     --username admin \
>     --firstname Loc \
>     --lastname Nguyen \
>     --role Admin \
>     --email admin@example.com \
>     --password admin

## SETUP CONNECT

## 🎮 Running the Application

### 1. Port Forwarding

Open a new terminal window and run the following command to create a tunnel from your machine to the Kubernetes cluster:

```bash
kubectl port-forward svc/airflow-api-server 8080:8080 --namespace orchestration
# Keep this terminal open while you use the app
```

### 2. Access the Interface

Once the port-forward is running, access the FastAPI Swagger UI by clicking the link below:  
👉 URL: http://localhost:8080  
👉 USER: admin  
👉 PASSWORD: admin

---

## 🖥️ Monitoring & Observability

### 1. Port Forwarding

Open a new terminal window and run the following command to create a tunnel from your machine to the Kubernetes cluster:

```bash
kubectl port-forward svc/monitor-stack-grafana 3000:80 -n monitoring
# Keep this terminal open while you use the app
```

### 2. Access the Interface

Once the port-forward is running, access the FastAPI Swagger UI by clicking the link below:  
👉 URL: http://localhost:3000  
👉 USER: admin  
👉 PASSWORD: (Get from secret):

```bash
kubectl get secret -n monitoring monitor-stack-grafana -o jsonpath="{.data.admin-password}" | base64 --decode
```

### 3. Import dashboard

1. In the Grafana sidebar, go to **Dashboards** -> **New** -> **Import**.

2. Click **Upload JSON file** and select the file located at: **assets/images/DB_Dashboard.json**.

3. Select **PostgreSQL** as the Data Source (ensure you have connected Postgres as a Data Source first).

4. Click **Import**.

---

## 🌊 Data Flow Pipeline

```mermaid
graph LR
    subgraph "Data Generation"
        A[Apache Airflow] -- "Simulate Transactions" --> B[(PostgreSQL Source)]
    end

    subgraph "Ingestion & Streaming"
        B -- "CDC (Log Mining)" --> C{Debezium}
        C -- "Events" --> D[Apache Kafka]
    end

    subgraph "Stream Processing & AI"
        D -- "Consume Stream" --> E[PyFlink Engine]
        E -- "Feature Engineering" --> F[XGBoost Model]
        F -- "Inference Result" --> E
    end

    subgraph "Serving & Monitoring"
        E -- "Sink Data" --> G[(PostgreSQL Sink)]
        G -- "Query" --> H[Grafana Dashboard]
        I[Prometheus] -- "Scrape Metrics" --> H
    end

    style D fill:#000,color:#fff
    style E fill:#E6522C,color:#fff
    style F fill:#8E75B2,color:#fff
    style H fill:#F46800,color:#fff
```

---

## 🚢 Demo Video

### Demo dashboard

![Demo dashboard](assets/videos/dashboard.gif)

### Demo airflow

![Demo airflow](assets/videos/airflow.gif)
