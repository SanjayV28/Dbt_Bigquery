{{ config(materialized='table') }}

with fhv_data as (
    select *, 
    from {{ ref('stg_fhvtripdata') }}
), 

dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

select
    fhv_data.dispatching_base_num as dispatching_base_num,
    fhv_data.pickup_datetime as pickup_datetime,
    fhv_data.dropoff_datetime as dropoff_datetime,
    fhv_data.pickup_locationid as pickup_locationid,
    fhv_data.dropoff_locationid as dropoff_locationid,
    fhv_data.sr_flag as sr_flag,
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone
from fhv_data
inner join dim_zones as pickup_zone
on fhv_data.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv_data.dropoff_locationid = dropoff_zone.locationid

