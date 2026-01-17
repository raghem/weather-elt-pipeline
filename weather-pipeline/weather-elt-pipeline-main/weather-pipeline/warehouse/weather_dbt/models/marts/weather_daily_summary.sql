select
  location_name,
  local_date,
  round(avg(temperature_2m)::numeric, 2) as avg_temp_c,
  round(min(temperature_2m)::numeric, 2) as min_temp_c,
  round(max(temperature_2m)::numeric, 2) as max_temp_c,
  round(sum(precipitation)::numeric, 2)  as total_precip
from {{ ref('stg_weather_hourly') }}
group by 1, 2

