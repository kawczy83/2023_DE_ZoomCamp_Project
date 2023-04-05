/*
    Creates a fact table for inspection data
*/

{{ config(materialized='table') }}

select
  lc.location_id,
  n.name_id,
  v.violation_id,
  inspection_date,
  grade_date,
  grade,
  score,-- non-additive fact
  bbl -- degenerate dimension
from {{ ref('stg_nyc_inspect') }} ip
join {{ref('dim_location') }} lc
    on lc.boro = ip.boro
    and lc.zipcode = ip.zipcode
    and lc.building = ip.building
    and lc.latitude = ip.latitude
    and lc.longitude = ip.longitude
join {{ref('dim_name') }} n 
    on n.camis = ip.camis
    and n.dba = ip.dba
join {{ref('dim_violation') }} v
    on v.critical_flag = ip.critical_flag
    and v.violation_code = ip.violation_code
