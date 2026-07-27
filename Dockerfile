# Sử dụng image Python nhẹ
FROM python:3.10-slim

# Thiết lập thư mục làm việc
WORKDIR /app

# Copy và cài đặt thư viện
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy toàn bộ mã nguồn
COPY consumer.py redis_service.py ./

# Chạy consumer
CMD ["python", "-u", "consumer.py"]
# Cờ -u (unbuffered) giúp print() đẩy log thẳng ra K8s log mà không bị delay