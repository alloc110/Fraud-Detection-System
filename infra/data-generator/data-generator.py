import os
import time
import random
import psycopg2
import uuid 
from faker import Faker
from datetime import datetime, timedelta
from typing import List, Tuple, Any

# ==========================================
# CẤU HÌNH BIẾN MÔI TRƯỜNG KUBERNETES
# ==========================================
DB_HOST = os.getenv("DB_HOST", "postgres-service")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "ecom_Db")
DB_USER = os.getenv("DB_USER", "hiveuser")
DB_PASS = os.getenv("DB_PASS", "hivepassword")
BATCH_SIZE = int(os.getenv("BATCH_SIZE_PER_SEC", "10000"))

# ==========================================
# CLASS LOGIC SINH DỮ LIỆU & GIAN LẬN
# ==========================================
class TransactionGenerator:
    """Class chuyên đảm nhiệm việc sinh dữ liệu giao dịch và các kịch bản gian lận."""
    def __init__(self):
        self.fake = Faker()
        self.payment_methods = ['CASH_IN', 'CASH_OUT', 'DEBIT', 'PAYMENT', 'TRANSFER']

    def _create_transaction(self, user: str = None, user_dest: str = None, 
                          amount: int = None, method: str = None, 
                          time_offset_seconds: int = 0) -> Tuple[Any, ...]:
        step = random.randint(1, 744)
        transaction_id = str(uuid.uuid4())
        
        amount = amount or random.randint(100_000, 5_000_000)
        method = method or random.choice(self.payment_methods)
        
        user = user or f"user_{random.randint(1000, 10000)}"
        user_dest = user_dest or f"user_{random.randint(1000, 9999)}"
        while user == user_dest:
            user_dest = f"user_{random.randint(1000, 10000)}"
            
        # Dùng thời gian hiện tại cộng thêm offset (dành cho kịch bản fraud liên tục)
        tx_time = datetime.now() + timedelta(seconds=time_offset_seconds)

        return (step, transaction_id, user, user_dest, amount, method, tx_time)

    def generate_normal_batch(self, batch_size: int = 10) -> List[Tuple[Any, ...]]:
        return [self._create_transaction() for _ in range(batch_size)]

    def generate_velocity_fraud(self, num_transactions: int = 20) -> List[Tuple[Any, ...]]:
        """Mô phỏng 1 user chuyển tiền liên tục nhiều lần trong thời gian ngắn"""
        hacker_user = f"user_{random.randint(1000, 10000)}"
        transactions = []
        for i in range(num_transactions):
            tx = self._create_transaction(
                user=hacker_user, amount=random.randint(50_000, 200_000_000), 
                method='TRANSFER', time_offset_seconds=(i * 2) 
            )
            transactions.append(tx)
        return transactions

    def generate_scattering_fraud(self, num_destinations: int = 10) -> List[Tuple[Any, ...]]:
        """Mô phỏng 1 user chuyển tiền đến rất nhiều tài khoản khác nhau"""
        compromised_user = f"user_{random.randint(1000, 10000)}"
        transactions = []
        for _ in range(num_destinations):
            tx = self._create_transaction(
                user=compromised_user, method='TRANSFER', time_offset_seconds=random.randint(0, 5)
            )
            transactions.append(tx)
        return transactions

    def generate_massive_fraud(self) -> List[Tuple[Any, ...]]:
        """Mô phỏng 1 giao dịch rút tiền mặt khổng lồ bất thường"""
        return [self._create_transaction(amount=random.randint(500_000_000, 2_000_000_000), method='CASH_OUT')]

    def generate_mixed_batch(self, total_size: int = 100) -> List[Tuple[Any, ...]]:
        """Trộn dữ liệu bình thường và dữ liệu gian lận theo mỗi BATCH"""
        data = []
        
        # Cố định số lượng giao dịch gian lận trong mỗi mẻ (tổng là 17 giao dịch)
        fraud_transactions = 5 + 10 + 2 
        normal_count = max(0, total_size - fraud_transactions)
        
        data.extend(self.generate_normal_batch(normal_count))
        data.extend(self.generate_velocity_fraud(num_transactions=5))
        data.extend(self.generate_scattering_fraud(num_destinations=10))
        data.extend(self.generate_massive_fraud())
        data.extend(self.generate_massive_fraud()) # Chèn 2 giao dịch rút tiền lớn
        
        random.shuffle(data)
        return data

# ==========================================
# KẾT NỐI DB VÀ VÒNG LẶP LIÊN TỤC CHO POD
# ==========================================
def get_connection():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
    )

def generate_data():
    generator = TransactionGenerator()
    conn = None
    
    while True:
        try:
            if conn is None or conn.closed:
                conn = get_connection()
                cursor = conn.cursor()
                print("✅ Đã kết nối tới PostgreSQL thành công!")

            # Sinh dữ liệu có chứa cả kịch bản Fraud
            transactions = generator.generate_mixed_batch(BATCH_SIZE)

            # Insert trực tiếp bằng SQL (không dùng file .sql riêng để dễ mount vào K8s)
            insert_sql = """
                INSERT INTO transactions (
                    step, transaction_id, source_user_id, dest_user_id, amount, payment_method, transaction_time
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            cursor.executemany(insert_sql, transactions)
            conn.commit()
            
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Đã chèn {len(transactions)} giao dịch (Đã trộn kịch bản Fraud).")
            
            # Nghỉ 1 giây để giả lập streaming
            time.sleep(1)

        except Exception as e:
            print(f"❌ Lỗi kết nối hoặc chèn dữ liệu: {e}")
            print("⏳ Thử kết nối lại sau 5 giây...")
            time.sleep(5)
            if conn:
                conn.close()
            conn = None

if __name__ == "__main__":
    print("🚀 Khởi động luồng sinh dữ liệu gian lận E-commerce...")
    generate_data()