import logging
from datetime import datetime
from enum import Enum
import xgboost as xgb
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
    CASH_IN = "CASH_IN"


PAYMENT_METHOD_ENCODING = {
    PaymentMethod.TRANSFER: 1,
    PaymentMethod.CASH_OUT: 2,
    PaymentMethod.PAYMENT: 0,
    PaymentMethod.DEBIT: 3,
    PaymentMethod.CASH_IN: 4,
}


class Transaction(BaseModel):
    step: int
    transaction_id: str
    source_user_id: str
    dest_user_id: str
    amount: float = Field(gt=0, description="Transaction amount, must be positive")
    payment_method: PaymentMethod
    transaction_time: int = Field(gt=0, description="Milliseconds since epoch")
    
    # Bổ sung các trường số dư (balance) theo yêu cầu hàm predict_fraud
    src_old_bal: float = Field(ge=0, description="Source account balance before transaction")
    src_new_bal: float = Field(ge=0, description="Source account balance after transaction")
    dest_old_bal: float = Field(ge=0, description="Destination account balance before transaction")
    dest_new_bal: float = Field(ge=0, description="Destination account balance after transaction")


@app.get("/")
def root():
    return {"status": "running"}


@app.get("/health")
def health():
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
        dt = datetime.fromtimestamp(tx.transaction_time / 1000)
        hour = dt.hour

        # Map tx_type (payment_method) sang giá trị số nguyên nếu mô hình yêu cầu dạng encoded
        tx_type_encoded = PAYMENT_METHOD_ENCODING[tx.payment_method]

        # Xây dựng DataFrame chứa đúng các đặc trưng theo chữ ký:
        # predict_fraud(amount, src_old_bal, src_new_bal, dest_old_bal, dest_new_bal, tx_type)
        df = pd.DataFrame([{
           "step": tx.step,
            "type": tx_type_encoded,
            "amount": tx.amount,
            "oldbalanceOrg": tx.src_old_bal,
            "newbalanceOrig": tx.src_new_bal,
            "oldbalanceDest": tx.dest_old_bal,
            "newbalanceDest": tx.dest_new_bal,
        }])
        dmatrix = xgb.DMatrix(df)
        
        proba = float(model.predict(dmatrix)[0])
        prediction = int(proba >= 0.5)
    except (ValueError, OverflowError, OSError) as e:
        logger.warning("Bad input for transaction %s: %s", tx.transaction_id, e)
        raise HTTPException(status_code=422, detail=f"Invalid input: {e}")

    except Exception as e:
        logger.error("Prediction failed for transaction %s: %s",
                     tx.transaction_id, e, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal model inference error")

    logger.info(
        "Scored transaction_id=%s prediction=%s proba=%.4f",
        tx.transaction_id,prediction,proba,
    )

    return {
        "transaction_id": tx.transaction_id,
        "prediction": prediction,
        "fraud_probability": proba,
    }