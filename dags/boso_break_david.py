from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import json
import csv
import os
import time

API_URL = "https://api.sampleapis.com/futurama/characters"  # API pública de ejemplo
BASE_DIR = "/tmp/data_api_bad"

def do_everything():
    now = datetime.now().strftime("%Y%m%d_%H%M%S")

    raw_dir = f"{BASE_DIR}/raw"
    processed_dir = f"{BASE_DIR}/processed"

    if not os.path.exists(raw_dir):
        os.makedirs(raw_dir)
    if not os.path.exists(processed_dir):
        os.makedirs(processed_dir)

    json_path = f"{raw_dir}/data_{now}.json"
    csv_path = f"{processed_dir}/data_{now}.csv"

    # ----------------------
    # 1. Llamar API
    # ----------------------
    print("Calling API...")
    response = requests.get(API_URL)
    data = response.json()

    with open(json_path, "w") as f:
        f.write(json.dumps(data))

    print(f"JSON written to {json_path}")

    # ----------------------
    # 2. Espera falsa (MALO)
    # ----------------------
    print("Esperando archivo...")
    time.sleep(15)

    # ----------------------
    # 3. Validar
    # ----------------------
    if not os.path.exists(json_path):
        raise Exception("File not found")

    if os.path.getsize(json_path) == 0:
        raise Exception("File is empty")

    with open(json_path, "r") as f:
        try:
            data = json.load(f)
        except:
            raise Exception("Invalid JSON")

    if not isinstance(data, list) or not data:
        raise Exception("JSON should be a non-empty list")

    for i, item in enumerate(data):
        if not all(k in item for k in ["id", "name", "age"]):
            raise Exception(f"Item {i} missing required keys")

    # ----------------------
    # 4. Transformar a CSV
    # ----------------------
    with open(csv_path, "w") as f:
        writer = csv.DictWriter(
    f,
    fieldnames=[
        "id",
        "name",
        "age",
        "gender",
        "occupation",
        "images",
        "sayings",
        "homePlanet",
        "species",
    ]
)

        writer.writeheader()
        for row in data:
            writer.writerow(row)

    print(f"CSV written to {csv_path}")
    print("DONE")

default_args = {}

dag = DAG(
    dag_id="etl_api_bad_practices",
    description="INTENTIONALLY bad DAG for training",
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
)

t1 = PythonOperator(
    task_id="do_everything",
    python_callable=do_everything,
    dag=dag,
)