FROM apache/airflow:2.8.1-python3.11

USER root

# Instalar dependencias del sistema si son necesarias
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Copiar requirements y instalar dependencias Python
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Crear directorios necesarios
RUN mkdir -p /opt/airflow/data/raw /opt/airflow/data/processed
