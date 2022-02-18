{{ config(materialized='view') }}

select 
    cast(dispatching_base_num as string) as dispatching_base_num,
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    cast(pulocationid as integer) as  pickup_locationid,
    cast(dolocationid as integer) as dropoff_locationid,
    cast(sr_flag as string) as sr_flag
from {{ source('staging','fhv_tripdata') }}


{% if var('is_test_run', default=false) %}

  limit 100

{% endif %}
