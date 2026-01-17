
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select location_name
from "weather"."public"."stg_weather_hourly"
where location_name is null



  
  
      
    ) dbt_internal_test