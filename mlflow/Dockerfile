FROM python:3.10-slim

# Install system dependencies required for building python packages and general utilities
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install MLflow and PostgreSQL adapter
# (Using psycopg2-binary avoids needing a full C compiler setup during installation)
RUN pip install --no-cache-dir mlflow psycopg2-binary

# Expose the default MLflow port
EXPOSE 5000

# Default command to run the MLflow tracking server
# Replace the backend-store-uri and default-artifact-root with your actual configurations
CMD ["mlflow", "server", "--host", "0.0.0.0", "--port", "5000"]