from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

with DAG(
    dag_id='start_flink_fraud_stream',
    start_date=datetime(2026, 3, 1),
    # Cứ mỗi 10 phút Airflow sẽ vào "ngó" Flink một lần
    schedule=timedelta(minutes=10), 
    catchup=False,
    max_active_runs=1, # Quan trọng: Chỉ cho phép 1 lần kiểm tra tại một thời điểm
) as dag:

    # Task này sẽ kiểm tra xem Job "FraudPrediction" có đang RUNNING không.
    # Nếu không thấy tên Job đó, nó mới chạy lệnh 'flink run'
      check_and_start_flink = BashOperator(
      task_id='check_and_start_fraud_job',
      bash_command="""
            JOB_NAME="FraudPrediction"
            # Tìm tên Pod JobManager
            JM_POD=$(kubectl get pods -n stream -l component=jobmanager -o jsonpath='{.items[0].metadata.name}')
            
            # Kiểm tra danh sách Job đang chạy trong Flink
            IS_RUNNING=$(kubectl exec -n stream $JM_POD -- flink list | grep "$JOB_NAME" | grep "RUNNING")
            
            if [ -z "$IS_RUNNING" ]; then
            echo "Job không chạy. Đang khởi động lại..."
            kubectl exec -n stream $JM_POD -- flink run -d -py /opt/flink/usrlib/fraud_prediction.py
            else
            echo "Job $JOB_NAME vẫn đang chạy tốt. Không cần làm gì cả."
            fi
            """
      )

      check_and_start_flink