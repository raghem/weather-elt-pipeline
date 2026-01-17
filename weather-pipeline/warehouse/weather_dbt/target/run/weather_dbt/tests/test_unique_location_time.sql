
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  select
  location_name,
  time,
  count(*) as n
from "weather"."public"."stg_weather_hourly"
group by 1, 2
having count(*) > 1
  
  
      
    ) dbt_internal_test