
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
from faker import Faker
import random

# Khởi tạo Faker
fake = Faker()

# Cấu hình kết nối Postgres (Cập nhật theo thông tin của bạn)
DB_CONFIG = {
    "host": "localhost",
    "database": "ecom_Db",
    "user": "hiveuser",
    "password": "hivepassword",
    "port": 5432
}

def insert_1000_users():
      conn = None
      try:
            pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn')
            conn = pg_hook.get_conn()
            cursor = conn.cursor()

            print("⏳ Đang chuẩn bị dữ liệu cho 9000 người dùng...")
            users_data = []
            for _ in range(1000, 10001):
                  user_id = f"user_{_}"
                  full_name = fake.name()
                  # Theo yêu cầu amount là VARCHAR(20)
                  amount = random.randint(100000, 50000000)
                  email = fake.email()
                  users_data.append((user_id, full_name, amount, email))

            # Sử dụng execute_values để chèn nhanh 9000 dòng
            query = "INSERT INTO users (user_id, full_name, amount, email) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING"
            cursor.executemany(query, users_data)

            conn.commit()
            print(f"✅ Đã tạo thành công {len(users_data)} người dùng vào PostgreSQL.")
            
            cursor.close()
            conn.close()

      except Exception as e:
            print(f"❌ Lỗi: {e}")
      finally:
            if conn:
                  conn.close()
                  
                  
                  
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
with DAG(
    'init_data_dag',
    default_args=default_args,
    description='Tạo dữ liệu ảo PaySim và đẩy vào Postgres Bronze Layer',
    start_date=datetime(2026, 2, 1),
    schedule_interval='@once',
    catchup=False
) as dag:

    generate_task = PythonOperator(
        task_id='generate_fake_users',
        python_callable=insert_1000_users,
        op_kwargs={'batch_size': 500} # Mỗi giờ tạo 500 giao dịch
    )