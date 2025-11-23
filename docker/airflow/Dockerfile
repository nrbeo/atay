FROM apache/airflow:3.1.0

ARG _AIRFLOW_VERSION=3.1.0

ADD docker/airflow/requirements.txt .

RUN pip install apache-airflow==${_AIRFLOW_VERSION} -r requirements.txt

ENV PYTHONPATH="/opt/airflow/etl:/opt/airflow:${PYTHONPATH}"

