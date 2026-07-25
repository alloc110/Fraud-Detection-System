INSERT INTO minio_transactions_parquet
SELECT 
    step, 
    transaction_id, 
    amount, 
    CASE 
        WHEN type = 'TRANSFER' THEN 4 
        WHEN type = 'CASH_OUT' THEN 1 
        ELSE 0 
    END,
    isFraud,
    DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd')
FROM kafka_transactions;