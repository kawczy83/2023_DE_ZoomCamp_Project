/*
    Creates a fact table for inspection data
*/

{{ config(materialized='table') }}

select
  lc.location_id,
  c.cuisine_id,
  v.violation_id,
  camis,
  inspection_date,
  record_date,
  action,
  inspection_type,
  critical_flag,
  grade,
  grade_date,
  score
from {{ ref('stg_nyc_inspect') }} stg
join {{ref('dim_location') }} lc
    on lc.boro = stg.boro
    and lc.building = stg.building
    and lc.street = stg.street
    and lc.zipcode = stg.zipcode
    and lc.phone = stg.phone
join {{ref('dim_cuisine') }} c 
    on c.cuisine_description = stg.cuisine_description
join {{ref('dim_violation') }} v
    on v.violation_code = stg.violation_code
