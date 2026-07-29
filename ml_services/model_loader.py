"""
model_loader.py

Loads an XGBoost model registered in MLflow, whose artifacts live in
MinIO (S3-compatible storage). Handles the two most common failure
modes explicitly instead of letting the app crash silently on import:
    1. MinIO/S3 credentials or connectivity issues
    2. Model name/stage/version not found in the MLflow registry

Environment variables expected (set these in your deployment):
    MLFLOW_TRACKING_URI   e.g. http://mlflow-server:5000
    MLFLOW_S3_ENDPOINT_URL  e.g. http://minio:9000   (MinIO endpoint)
    AWS_ACCESS_KEY_ID       MinIO access key
    AWS_SECRET_ACCESS_KEY   MinIO secret key
    MODEL_NAME              registered model name in MLflow, e.g. "fraud-xgb"
    MODEL_STAGE             "Production" / "Staging" / or a version number
"""

import logging
import os

import mlflow
import mlflow.xgboost

from mlflow import MlflowClient

client = MlflowClient()
logger = logging.getLogger("model_loader")

MODEL_NAME = os.environ.get("MODEL_NAME", "fraud-model")
MODEL_VERSION = os.environ.get("MODEL_VERSION")
MODEL_ALIAS = os.environ.get("MODEL_ALIAS")

versions = client.search_model_versions(
    f"name='{MODEL_NAME}'"
)

latest = max(versions, key=lambda v: int(v.version))

model_uri = f"models:/{MODEL_NAME}/{latest.version}"
class ModelLoadError(Exception):
    """Raised when the model cannot be loaded from MLflow/MinIO."""

def load_model():
    """
    Loads the model and returns it. Raises ModelLoadError on any failure
    so the caller (FastAPI app) can decide how to handle a broken startup
    instead of crashing with an unhandled traceback.
    """
    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI")
    if not tracking_uri:
        raise ModelLoadError("MLFLOW_TRACKING_URI env var is not set")

    mlflow.set_tracking_uri(tracking_uri)

    model_uri = f"models:/{MODEL_NAME}/{latest.version}"

    try:
        logger.info("Loading model from %s (tracking_uri=%s)", model_uri, tracking_uri)
        model = mlflow.xgboost.load_model(model_uri)
        logger.info("Model loaded successfully: %s", model_uri)
        return model
    except mlflow.exceptions.MlflowException as e:
        raise ModelLoadError(
            f"Could not find model '{MODEL_NAME}' at version '{versions}' "
            f"in MLflow registry: {e}"
        ) from e
    except Exception as e:
        # Covers: MinIO/S3 connectivity, credentials, network errors, etc.
        raise ModelLoadError(
            f"Failed to load model artifacts (check MinIO/S3 connectivity "
            f"and credentials): {e}"
        ) from e