import logging
from datetime import datetime
from enum import Enum

import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from model_loader import ModelLoadError, load_model

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("fraud-api")

app = FastAPI(
    title="Fraud Detection API",
    version="1.0.0",
)

# ---------------------------------------------------------------------------
# Model loading: don't let a bad load crash the whole app at import time.
# Instead, keep `model = None` and let /health and /predict report the
# real state, so the process can still start (useful behind k8s readiness
# probes / for debugging) instead of crash-looping with no visibility.
# ---------------------------------------------------------------------------
model = None
model_load_error = None

try:
    model = load_model()
except ModelLoadError as e:
    model_load_error = str(e)
    logger.error("Model failed to load at startup: %s", model_load_error)


class PaymentMethod(str, Enum):
    TRANSFER = "TRANSFER"
    CASH_OUT = "CASH_OUT"
    PAYMENT = "PAYMENT"
    DEBIT = "DEBIT"


PAYMENT_METHOD_ENCODING = {
    PaymentMethod.TRANSFER: 0,
    PaymentMethod.CASH_OUT: 1,
    PaymentMethod.PAYMENT: 2,
    PaymentMethod.DEBIT: 3,
}


class Transaction(BaseModel):
    step: int
    transaction_id: str
    source_user_id: str
    dest_user_id: str
    amount: float = Field(gt=0, description="Transaction amount, must be positive")
    payment_method: PaymentMethod
    # Milliseconds since epoch. Change the divisor in `predict()` below if
    # your upstream (Flink) sends a different unit.
    transaction_time: int = Field(gt=0)


@app.get("/")
def root():
    return {"status": "running"}


@app.get("/health")
def health():
    """
    Real health check: reports whether the model is actually loaded and
    usable, not just whether the process is alive.
    """
    if model is None:
        raise HTTPException(
            status_code=503,
            detail={
                "status": "unhealthy",
                "reason": model_load_error or "model not loaded",
            },
        )
    return {"status": "ok", "model_loaded": True}


@app.post("/predict")
def predict(tx: Transaction):
    if model is None:
        raise HTTPException(
            status_code=503,
            detail=f"Model unavailable: {model_load_error or 'not loaded'}",
        )

    try:
        # NOTE: assumes tx.transaction_time is milliseconds since epoch.
        # Adjust the divisor here if your upstream sends seconds or
        # microseconds instead — this must match whatever Flink sends.
        dt = datetime.fromtimestamp(tx.transaction_time / 1000)
        hour = dt.hour

        payment_method_encoded = PAYMENT_METHOD_ENCODING[tx.payment_method]

        df = pd.DataFrame([{
            "amount": tx.amount,
            "hour": hour,
            "payment_method": payment_method_encoded,
        }])

        prediction = model.predict(df)
        proba = model.predict_proba(df)[0][1]

    except (ValueError, OverflowError, OSError) as e:
        # e.g. transaction_time out of range for datetime.fromtimestamp
        logger.warning("Bad input for transaction %s: %s", tx.transaction_id, e)
        raise HTTPException(status_code=422, detail=f"Invalid input: {e}")

    except Exception as e:
        # Catch-all so a model/inference error returns a clean 500
        # instead of an unhandled traceback leaking to the client.
        logger.error("Prediction failed for transaction %s: %s",
                     tx.transaction_id, e, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal model inference error")

    logger.info(
        "Scored transaction_id=%s prediction=%s proba=%.4f",
        tx.transaction_id, int(prediction[0]), float(proba),
    )

    return {
        "transaction_id": tx.transaction_id,
        "user": tx.source_user_id,
        "prediction": int(prediction[0]),
        "fraud_probability": float(proba),
    }