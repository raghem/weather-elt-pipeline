import requests
import psycopg2
from psycopg2.extras import execute_values

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "weather",
    "user": "postgres",
    "password": "postgres",
}

def fetch_open_meteo_hourly(latitude: float, longitude: float, timezone: str):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": "temperature_2m,relative_humidity_2m,precipitation",
        "timezone": timezone,
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def fetch_locations_from_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT location_name, latitude, longitude, timezone
            FROM locations
            ORDER BY location_name
        """)
        return cur.fetchall()

def main():
    insert_sql = """
        INSERT INTO weather_hourly_raw
        (location_name, latitude, longitude, time, temperature_2m, relative_humidity_2m, precipitation)
        VALUES %s
        ON CONFLICT (location_name, time)
        DO UPDATE SET
          latitude = EXCLUDED.latitude,
          longitude = EXCLUDED.longitude,
          temperature_2m = EXCLUDED.temperature_2m,
          relative_humidity_2m = EXCLUDED.relative_humidity_2m,
          precipitation = EXCLUDED.precipitation,
          ingested_at = NOW();
    """

    conn = psycopg2.connect(**DB_CONFIG)
    try:
        locations = fetch_locations_from_db(conn)
        if not locations:
            raise RuntimeError("No locations found in DB. Insert rows into the locations table first.")

        total = 0
        for (name, lat, lon, tz) in locations:
            data = fetch_open_meteo_hourly(lat, lon, tz)

            hourly = data.get("hourly", {})
            times = hourly.get("time", [])
            temps = hourly.get("temperature_2m", [])
            humidity = hourly.get("relative_humidity_2m", [])
            precip = hourly.get("precipitation", [])

            if not times:
                print(f"No hourly data returned for {name}. Skipping.")
                continue

            rows = []
            for i, t in enumerate(times):
                rows.append((
                    name, lat, lon, t,
                    temps[i] if i < len(temps) else None,
                    humidity[i] if i < len(humidity) else None,
                    precip[i] if i < len(precip) else None,
                ))

            with conn:
                with conn.cursor() as cur:
                    execute_values(cur, insert_sql, rows, page_size=500)

            total += len(rows)
            print(f"Inserted/updated {len(rows)} hourly rows for {name}.")

        print(f"Done. Total rows processed: {total}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()

