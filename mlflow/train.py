import os
import mlflow

# Trỏ tới service nội bộ của cluster
os.environ["MLFLOW_S3_ENDPOINT_URL"] = "http://minio-service.ml-model.svc.cluster.local:9000"
os.environ["AWS_ACCESS_KEY_ID"] = "admin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "admin123"

mlflow.set_tracking_uri("http://mlflow-service.ml-model.svc.cluster.local:5000")
mlflow.set_experiment("fraud_detection")

with mlflow.start_run():
    # ... code train model ...
    
    # Log model lên MLflow (nó sẽ tự đẩy file sang bucket của MinIO)
    mlflow.sklearn.log_model(sk_model=model, artifact_path="fraud_model")