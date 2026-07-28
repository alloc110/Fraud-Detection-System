-- 1. Bảng Khách hàng
CREATE TABLE users (
    user_id VARCHAR(50) PRIMARY KEY,
    full_name VARCHAR(100),
    amount BIGINT,
    email VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE DATABASE mlflow_db;

-- 3. Bảng Giao dịch (Nơi luồng data tuôn chảy liên tục)
DROP TABLE IF EXISTS transactions CASCADE;
CREATE TABLE transactions (
    step INT,
    transaction_id VARCHAR(50) PRIMARY KEY,
    source_user_id VARCHAR(50) REFERENCES users(user_id),
    dest_user_id VARCHAR(50) REFERENCES users(user_id),
    amount BIGINT,
    payment_method VARCHAR(50),
    transaction_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 4. Bảng Cảnh báo gian lận (Đích đến để Flink ghi kết quả)
CREATE TABLE fraud_alerts (
    transaction_id VARCHAR(255) PRIMARY KEY,
    user_id VARCHAR(255),
    amount DOUBLE PRECISION,
    payment_method VARCHAR(50),
    is_fraud_predicted INTEGER, -- Máy dự đoán (0 hoặc 1)
    
    -- HAI CỘT LỘC CẦN THÊM --
    actual_label INTEGER DEFAULT NULL, -- Con người xác nhận lại (0: Thật, 1: Gian lận)
    status VARCHAR(20) DEFAULT 'PENDING', -- Trạng thái (PENDING, VERIFIED, CLOSED)
    
    alert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);