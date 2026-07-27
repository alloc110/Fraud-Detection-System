import time
import redis
import os

class RedisFraudService:
    def __init__(self):
        # Lấy từ biến môi trường, dự phòng giá trị mặc định nếu không khai báo
        host = os.getenv('REDIS_HOST', 'redis')
        port = int(os.getenv('REDIS_PORT', 6379))
        
        # Đưa cả config logic ra ngoài để dễ chỉnh sửa luật gian lận
        self.window_size = int(os.getenv('FRAUD_WINDOW_SIZE', 300))
        self.fraud_threshold = int(os.getenv('FRAUD_THRESHOLD', 5))

        self.client = redis.Redis(host=host, port=port, decode_responses=True)

    def check_and_record(self, user_id: str) -> dict:
        current_time = int(time.time())
        current_minute_bucket = current_time - (current_time % 60)
        
        redis_key = f"fraud:counter:{user_id}:{current_minute_bucket}"
        
        pipe = self.client.pipeline()
        pipe.incr(redis_key)
        pipe.expire(redis_key, self.window_size)
        
        keys_to_check = [
            f"fraud:counter:{user_id}:{current_minute_bucket - (i * 60)}"
            for i in range(int(self.window_size / 60))
        ]
        pipe.mget(keys_to_check)
        
        results = pipe.execute()
        
        values = results[2] 
        total_tx = sum(int(v) for v in values if v is not None)
        
        is_fraud = total_tx >= self.fraud_threshold
        
        return {
            "user_id": user_id,
            "total_transactions_5m": total_tx,
            "is_fraud": is_fraud
        }