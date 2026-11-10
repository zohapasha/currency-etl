from datetime import datetime, timezone
import os

import requests
from pymongo import MongoClient
from airflow.decorators import dag, task

API_URL = "https://api.frankfurter.dev/v2/rates"
MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:admin123@mongo:27017/")


@dag(
    dag_id="currency_exchange_etl",
    start_date=datetime(2026, 11, 1),
    schedule="@daily",
    catchup=False,
    tags=["etl", "currency"],
)
def currency_exchange_etl():

    @task
    def extract_rates(**context):
        """Task 1: call the Frankfurter API and push the raw JSON to a named XCom."""
        resp = requests.get(
            API_URL,
            params={"base": "EUR", "quotes": "USD,GBP,JPY"},
            timeout=30,
        )
        resp.raise_for_status()
        raw = resp.json()
        print("Raw API response:", raw)
        context["ti"].xcom_push(key="raw_rates", value=raw)

    @task
    def transform_rates(**context):
        """Task 2: pull raw rates by key, reshape them, push the result to a named XCom."""
        ti = context["ti"]
        raw = ti.xcom_pull(task_ids="extract_rates", key="raw_rates")
        retrieved_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

        transformed = [
            {
                "base_currency": r["base"],
                "target_currency": r["quote"],
                "exchange_rate": r["rate"],
                "rate_date": r["date"],
                "source": "Frankfurter API",
                "retrieved_at": retrieved_at,
            }
            for r in raw
        ]
        print("Transformed records:", transformed)
        ti.xcom_push(key="transformed_rates", value=transformed)

    @task
    def load_to_mongo(**context):
        """Task 3: pull transformed records by key and upsert them into MongoDB."""
        ti = context["ti"]
        records = ti.xcom_pull(task_ids="transform_rates", key="transformed_rates")

        client = MongoClient(MONGO_URI)
        collection = client["currency_db"]["exchange_rates"]
        for rec in records:
            # upsert so re-running the same day doesn't create duplicates
            collection.update_one(
                {
                    "base_currency": rec["base_currency"],
                    "target_currency": rec["target_currency"],
                    "rate_date": rec["rate_date"],
                },
                {"$set": rec},
                upsert=True,
            )
        client.close()
        print(f"Loaded {len(records)} records into currency_db.exchange_rates")

    extract_rates() >> transform_rates() >> load_to_mongo()


currency_exchange_etl()