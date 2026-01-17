
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select local_date
from "weather"."public"."stg_weather_hourly"
where local_date is null



  
  
      
    ) dbt_internal_test