
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select local_hour
from "weather"."public"."stg_weather_hourly"
where local_hour is null



  
  
      
    ) dbt_internal_test