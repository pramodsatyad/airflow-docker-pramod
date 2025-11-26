FROM apache/airflow:2.10.1

# Switch to root to install system dependencies
USER root

# Install system dependencies needed for compilation
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    python3-dev \
    libpq-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Install Python packages with PINNED versions to avoid dependency resolution hell
RUN pip install --no-cache-dir \
    apache-airflow-providers-snowflake==5.7.0 \
    snowflake-connector-python==3.12.1 \
    yfinance==0.2.66 \
    dbt-core==1.8.8 \
    dbt-snowflake==1.8.4