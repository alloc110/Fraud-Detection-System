import pandas as pd
import xgboost as xgb
import os
import pandas as pd
import numpy as np
from pyflink.table.udf import udf
from pyflink.table import DataTypes
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

settings = EnvironmentSettings.new_instance() \
    .in_streaming_mode() \
    .build()

# 2. Tạo Table Environment
t_env = StreamTableEnvironment.create(environment_settings=settings)
t_env.get_config().set("python.execution-mode", "process")


# (Tùy chọn) Cấu hình để Python UDF chạy mượt hơn trong K8s
t_env.get_config().set("python.executable", "python3")

MODEL_PATH = "/opt/flink/usrlib/fraud_model.json"

# 2. Khởi tạo mô hình Global để tránh việc mỗi dòng dữ liệu lại load lại file (gây chậm)
_model = None

def get_model():
    global _model
    if _model is None:
        _model = xgb.XGBClassifier()
        if os.path.exists(MODEL_PATH):
            _model.load_model(MODEL_PATH)
        else:
            # Fallback nếu không tìm thấy file để tránh crash Job Flink
            print(f"Warning: Model file not found at {MODEL_PATH}")
    return _model

# 3. Mapping loại giao dịch (Phải khớp 100% với lúc Lộc train)
TYPE_MAP = {
    'PAYMENT': 0,
    'TRANSFER': 1, 
    'CASH_OUT': 2,
    'DEBIT': 3, 
    'CASH_IN': 4
}

@udf(result_type=DataTypes.INT(), 
     input_types=[DataTypes.INT(), DataTypes.STRING(), DataTypes.BIGINT(), 
                  DataTypes.BIGINT(), DataTypes.BIGINT(), DataTypes.BIGINT(), DataTypes.BIGINT()])
def predict_fraud(step, type_str, amount, oldbalanceOrg, newbalanceOrig, oldbalanceDest, newbalanceDest):
    """
    Hàm UDF để dự đoán gian lận cho từng dòng dữ liệu từ Flink SQL
    """
    # Lấy mô hình
    model = get_model()
    if model is None:
        return 0

    try:
        type_code = TYPE_MAP.get(type_str, 0)
        # Sử dụng NumPy để đạt tốc độ cao nhất trong Flink
        features = np.array([[step, type_code, int(amount), 
                              int(oldbalanceOrg), int(newbalanceOrig), 
                              int(oldbalanceDest), int(newbalanceDest)]])
        
        prediction = model.predict(features)
        return int(prediction[0])
    except Exception as e:
        return 0
    
t_env.create_temporary_system_function("predict_fraud", predict_fraud) 

# Khai báo nguồn Kafka
t_env.execute_sql("""
    CREATE TABLE IF NOT EXISTS kafka_transactions (
        step INT,
        transaction_id STRING,
        user_id STRING,
        dest_user_id STRING,
        amount BIGINT,
        payment_method STRING,
        proctime AS PROCTIME()
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'pg.public.transactions',
        'properties.bootstrap.servers' = 'my-cluster-kafka-bootstrap:9092', 
        'properties.group.id' = 'flink_production_group',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'debezium-json','debezium-json.schema-include' = 'true', -- THÊM DÒNG NÀY VÀO ĐÂY
        'debezium-json.ignore-parse-errors' = 'true' -- Thêm dòng này để bỏ qua nếu có tin nhắn lỗi
    )
""")

# Khai báo nguồn Postgres (Lookup Table)
t_env.execute_sql("""
    CREATE TABLE IF NOT EXISTS pg_users (
        user_id STRING,
        amount BIGINT, -- Khai báo đúng kiểu VARCHAR bên Postgres
        -- Tạo cột số để dùng cho XGBoost
        current_balance AS amount,
        PRIMARY KEY (user_id) NOT ENFORCED
    ) WITH (
        'connector' = 'jdbc',
        'url' = 'jdbc:postgresql://postgres-service.data-storage:5432/ecom_Db', 
        'table-name' = 'users',
        'username' = 'hiveuser',
        'password' = 'hivepassword'
    )
""")

t_env.execute_sql("""
    CREATE TABLE IF NOT EXISTS fraud_results_sink (
        transaction_id STRING,
        user_id STRING,
        amount DOUBLE,
        payment_method STRING,
        is_fraud_predicted INT,
        alert_time TIMESTAMP(3),
        PRIMARY KEY (transaction_id) NOT ENFORCED
    ) WITH (
        'connector' = 'jdbc',
        'url' = 'jdbc:postgresql://postgres-service.data-storage:5432/ecom_Db', 
        'table-name' = 'fraud_alerts', -- Ghi vào bảng mới có cột feedback
        'username' = 'hiveuser',
        'password' = 'hivepassword',
        'sink.buffer-flush.max-rows' = '1' -- Đẩy dữ liệu đi ngay lập tức khi có kết quả
    )
""")

# 5. Thực hiện truy vấn Flink SQL để:
t_env.execute_sql("""
    INSERT INTO fraud_results_sink
    SELECT 
        transaction_id,
        user_id,
        CAST(transaction_amount AS DOUBLE),
        payment_method,
        predict_fraud(
            step, 
            payment_method, 
            transaction_amount, 
            SOURCE_USER_OLD_BALANCE, 
            SOURCE_USER_NEW_BALANCE, 
            DEST_USER_OLD_BALANCE, 
            DEST_USER_NEW_BALANCE
        ) AS is_fraud_predicted,
        CURRENT_TIMESTAMP AS alert_time
    FROM (
        SELECT 
            t.step, t.transaction_id, t.user_id, t.amount AS transaction_amount, t.payment_method,
            COALESCE(u_sor.amount, 0) AS SOURCE_USER_OLD_BALANCE,
            CASE 
                WHEN t.payment_method IN ('CASH_OUT', 'TRANSFER', 'DEBIT') THEN COALESCE(u_sor.amount, 0) - t.amount 
                ELSE COALESCE(u_sor.amount, 0) + t.amount 
            END AS SOURCE_USER_NEW_BALANCE,
            COALESCE(u_des.amount, 0) AS DEST_USER_OLD_BALANCE,
            CASE 
                WHEN t.payment_method IN ('CASH_OUT', 'TRANSFER', 'DEBIT') THEN COALESCE(u_des.amount, 0) + t.amount 
                ELSE COALESCE(u_des.amount, 0) - t.amount
            END AS DEST_USER_NEW_BALANCE
        FROM kafka_transactions t
        LEFT JOIN pg_users FOR SYSTEM_TIME AS OF t.proctime AS u_sor ON t.user_id = u_sor.user_id
        LEFT JOIN pg_users FOR SYSTEM_TIME AS OF t.proctime AS u_des ON t.dest_user_id = u_des.user_id
    )
""")