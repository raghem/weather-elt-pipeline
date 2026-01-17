# Weather ELT Pipeline (API → Postgres → dbt)

## Overview
This project ingests hourly weather data from the Open-Meteo API and loads it into Postgres (raw layer).  
dbt then transforms the raw data into clean staging models and an analytics-ready daily summary mart.

## Architecture
**API → Python ingestion → Postgres (raw) → dbt (staging/marts) → analytics**

## Tech Stack
- Python (requests, psycopg2)
- Postgres (Docker)
- dbt (ELT transformations + tests)

## Data Model
### Raw
- `weather_hourly_raw`: hourly API data (idempotent upserts)
- `locations`: list of locations to ingest

### Staging (dbt)
- `stg_weather_hourly` (view): cleaned hourly weather with local_date/local_hour

### Mart (dbt)
- `weather_daily_summary` (table): daily avg/min/max temperature + total precipitation by location

## How to Run Locally
### 1) Start Postgres
```bash
docker compose up -d

