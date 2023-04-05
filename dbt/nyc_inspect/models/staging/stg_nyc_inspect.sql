/*
    This will stage the nyc restaurant inspection data
*/

{{ config(materialized='view') }}

SELECT
  
 camis,
  dba,
  action as action,
  boro,
  building,
  street,
  zipcode,
  phone,
  cuisine_description,
  latitude,
  longitude,
  inspection_date,
  grade_date,
  record_date,
  violation_code,
  violation_description,
  critical_flag,
  score,
  grade,
  inspection_type,
  community_board,
  council_district,
  census_tract,
  bin,
  bbl,
  nta
from {{ source('staging','nyc_inspect_data_partitioned_clustered') }}