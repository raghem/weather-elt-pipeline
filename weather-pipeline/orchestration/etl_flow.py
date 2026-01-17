from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from datetime import timedelta
import subprocess
import os


@task(retries=2, retry_delay_seconds=30)
def run_ingestion():
    logger = get_run_logger()
    logger.info("Running ingestion: python app/ingest_weather.py")

    result = subprocess.run(
        ["python", "app/ingest_weather.py"],
        check=False,
        capture_output=True,
        text=True,
    )

    logger.info(result.stdout.strip() or "(no stdout)")
    if result.returncode != 0:
        logger.error(result.stderr.strip() or "(no stderr)")
        raise RuntimeError("Ingestion step failed")


@task(retries=1, retry_delay_seconds=10)
def run_dbt(cmd: list[str]):
    logger = get_run_logger()
    logger.info(f"Running dbt command: {' '.join(cmd)}")

    # Ensure we run inside your dbt project directory
    dbt_dir = os.path.join("warehouse", "weather_dbt")

    result = subprocess.run(
        cmd,
        cwd=dbt_dir,
        check=False,
        capture_output=True,
        text=True,
    )

    logger.info(result.stdout.strip() or "(no stdout)")
    if result.returncode != 0:
        logger.error(result.stderr.strip() or "(no stderr)")
        raise RuntimeError(f"dbt step failed: {' '.join(cmd)}")


@flow(name="weather-elt-pipeline")
def weather_elt_pipeline():
    run_ingestion()
    run_dbt(["dbt", "run"])
    run_dbt(["dbt", "test"])


if __name__ == "__main__":
    weather_elt_pipeline()

