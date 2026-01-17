with source as (

    select *
    from "weather"."public"."weather_hourly_raw"

),

final as (

    select
        location_name,
        latitude,
        longitude,
        time,
        (time at time zone 'America/Chicago')::date as local_date,
        extract(hour from (time at time zone 'America/Chicago'))::int as local_hour,
        temperature_2m,
        relative_humidity_2m,
        precipitation,
        ingested_at
    from source

)

select * from final