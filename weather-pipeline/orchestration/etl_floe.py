import os
import requests
from dotenv import load_dotenv

load_dotenv()

from prefect import task, flow, get_run_logger

@task
def telegram_notify(message: str):
    logger = get_run_logger()
    token = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
    chat_id = (os.getenv("TELEGRAM_CHAT_ID") or "").strip()

    if not token or not chat_id:
        raise RuntimeError("Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID")

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    r = requests.post(url, json={"chat_id": chat_id, "text": message}, timeout=20)
    logger.info(f"Telegram response: {r.status_code} {r.text}")
    r.raise_for_status()

@flow(name="weather-elt-pipeline")
def weather_elt_pipeline():
    try:
        run_ingestion()
        run_dbt(["dbt", "run"])
        run_dbt(["dbt", "test"])
        telegram_notify("✅ Weather ELT: SUCCESS (ingestion + dbt run + dbt test)")
    except Exception as e:
        telegram_notify(f"❌ Weather ELT: FAILED\nError: {e}")
        raise

