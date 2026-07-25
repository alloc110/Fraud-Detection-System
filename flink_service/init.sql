-- =================================================================
-- 1. CÀI ĐẶT HỆ THỐNG & KẾT NỐI S3 (MINIO)
-- =================================================================
-- SET 'table.exec.resource.default-parallelism' = '1';
-- SET 'sql-client.execution.result-mode' = 'table';

-- -- Cấu hình để Flink có thể ghi file vào MinIO
-- SET 's3.endpoint' = 'http://my-minio.data-storage.svc.cluster.local:9000';
-- SET 's3.access-key' = 'hiveuser'; 
-- SET 's3.secret-key' = 'hivepassword';
-- SET 's3.path.style.access' = 'true';

-- =================================================================
-- 2. BẢNG KAFKA (DỮ LIỆU GIAO DỊCH REAL-TIME)
-- =================================================================
DROP TABLE IF EXISTS kafka_transactions;
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
    'format' = 'debezium-json','debezium-json.schema-include' = 'true', 
    'debezium-json.ignore-parse-errors' = 'true' 
 );



-- =================================================================
-- 3. BẢNG POSTGRES (DÙNG ĐỂ TRA CỨU SỐ DƯ - LOOKUP)
-- =================================================================
DROP TABLE IF EXISTS pg_users;
CREATE TABLE IF NOT EXISTS pg_users (
    user_id STRING,
    amount BIGINT,
    current_balance AS amount,
    PRIMARY KEY (user_id) NOT ENFORCED
 ) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres-service.data-storage:5432/ecom_Db', 
    'table-name' = 'users',
    'username' = 'hiveuser',
    'password' = 'hivepassword'
);

-- =================================================================
-- 4. BẢNG MINIO (LƯU TRỮ PARQUET VĨNH VIỄN)
-- =================================================================
-- CREATE TABLE IF NOT EXISTS minio_transactions_parquet (
--     step INT,
--     transaction_id STRING,
--     amount DOUBLE,
--     type_code INT, 
--     isFraud INT,
--     dt STRING
-- ) PARTITIONED BY (dt)
-- WITH (
--     'connector' = 'filesystem',
--     'path' = 's3a://flink-data/transactions_archive/',
--     'format' = 'parquet'
-- );


-- =================================================================
-- 5. SELECT TEST (KIỂM TRA JOIN GIỮA KAFKA VÀ POSTGRES)
-- =================================================================
SELECT 
    t.transaction_id,
    t.user_id AS user_id,
    t.amount AS transaction_amount,
    u_sor.current_balance AS SOURCE_USER_OLD_BALANCE, 
    CASE 
        WHEN t.payment_method = 'CASH_OUT' OR t.payment_method = 'TRANSFER' OR t.payment_method = 'DEBIT' THEN u_sor.current_balance - t.amount 
        ELSE (u_sor.current_balance + t.amount) -- Nếu là rút tiền hoặc chuyển tiền thì số dư mới sẽ là số dư cũ trừ đi số tiền giao dịch
    END AS SOURCE_USER_NEW_BALANCE,
    u_des.current_balance AS DEST_USER_OLD_BALANCE,
    CASE 
        WHEN t.payment_method = 'CASH_OUT' OR t.payment_method = 'TRANSFER' OR t.payment_method = 'DEBIT' THEN u_des.current_balance + t.amount 
        ELSE (u_des.current_balance - t.amount)
    END AS DEST_USER_NEW_BALANCE,
    t.payment_method as TYPE
FROM kafka_transactions t
LEFT JOIN pg_users FOR SYSTEM_TIME AS OF t.proctime AS u_sor -- Temporal Join
ON t.user_id = u_sor.user_id
LEFT JOIN pg_users FOR SYSTEM_TIME AS OF t.proctime AS u_des -- Temporal Join
ON t.dest_user_id = u_des.user_id;

-- =================================================================
-- 6. ĐĂNG KÝ HÀM ML XGBOOST
-- =================================================================
CREATE TEMPORARY FUNCTION IF NOT EXISTS predict_fraud 
AS 'fraud_prediction.predict_fraud' 
LANGUAGE PYTHON;