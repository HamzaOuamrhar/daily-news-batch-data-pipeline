# .env example

## Image version for Airflow
AIRFLOW_IMAGE_NAME=apache/airflow:3.0.2

## PostgreSQL credentials
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow

## Local project directory (used to mount dags/, logs/, config/, plugins/)
AIRFLOW_PROJ_DIR=.

## Optional: Airflow Web UI credentials
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow

## Optional: Additional Python packages to install
_PIP_ADDITIONAL_REQUIREMENTS=

## UID to avoid permission issues (especially on Linux)
AIRFLOW_UID=50000
