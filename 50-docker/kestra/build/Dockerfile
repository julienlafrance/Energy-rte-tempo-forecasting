FROM kestra/kestra:latest
USER root
RUN apt-get update && apt-get install -y git jq postgresql-client && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir \
    dbt-postgres \
    numpy \
    scipy \
    psycopg2-binary \
    requests \
    statsmodels \
    boto3 \
    pyarrow \
    pandas \
    scikit-learn \
    mlflow \
    matplotlib \
    sqlalchemy \
    "elasticsearch>=8.0.0,<9.0.0"
USER kestra
