import json
import os
from kafka import KafkaConsumer
from redis_service import RedisFraudService

# 1. Khởi tạo service Redis (đã tự ăn biến môi trường)
redis_service = RedisFraudService()

# 2. Lấy cấu hình Kafka
KAFKA_BROKER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
TOPIC_NAME = os.getenv('KAFKA_TOPIC', 'finhouse.public.transactions')
GROUP_ID = os.getenv('KAFKA_GROUP_ID', 'redis-fraud-detector-group')

print(f"🚀 Kết nối Kafka: {KAFKA_BROKER} | Topic: {TOPIC_NAME}", flush=True)

# 3. Khởi tạo Kafka Consumer
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id=GROUP_ID,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None
)

print(f"✅ Đang lắng nghe sự kiện...", flush=True)

for message in consumer:
    if not message.value:
        continue

    payload = message.value.get('payload', {})
    if payload.get('op') != 'c':
        continue

    new_record = payload.get('after')
    if not new_record:
        continue

    user_id = new_record.get('source_user_id')
    tx_id = new_record.get('transaction_id')

    if not user_id:
        continue

    # Chạy logic kiểm tra gian lận
    result = redis_service.check_and_record(user_id)

    if result["is_fraud"]:
        print(f"🚨 [CẢNH BÁO GIAN LẬN] User: {user_id} | Giao dịch: {tx_id} | "
              f"Tần suất: {result['total_transactions_5m']} lần/5 phút!", flush=True)
    else:
        print(f"✅ Hợp lệ | User: {user_id} | Giao dịch: {tx_id} | "
              f"Tần suất: {result['total_transactions_5m']} lần/5 phút.", flush=True)