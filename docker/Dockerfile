FROM apache/airflow:3.3.2-python3.12

ARG AIRFLOW_VERSION=3.3.2
ARG AIRFLOW_CONSTRAINTS="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.12.txt"

USER airflow

COPY --chown=airflow:root pyproject.toml README.md /tmp/build/
COPY --chown=airflow:root src /tmp/build/src

RUN pip install --no-cache-dir --constraint "${AIRFLOW_CONSTRAINTS}" /tmp/build \
    && rm -rf /tmp/build
