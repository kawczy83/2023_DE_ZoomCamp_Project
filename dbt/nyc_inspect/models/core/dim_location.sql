/*
    Creates a dimension table for location
*/

{{ config(materialized='table') }}

select
    row_number() over (order by boro) as location_id,
    boro,
    building,
    street,
    zipcode,
    latitude,
    longitude
from (
    select
      distinct boro,
      building,
      street,
      zipcode,
      latitude,
      longitude
   from {{ ref('stg_nyc_inspect') }}
    )
where boro <> '0'

