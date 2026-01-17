select
  location_name,
  time,
  count(*) as n
from {{ ref('stg_weather_hourly') }}
group by 1, 2
having count(*) > 1

