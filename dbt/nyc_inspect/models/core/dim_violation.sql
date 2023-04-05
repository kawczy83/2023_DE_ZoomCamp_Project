/*
    Creates a dimension table for violations
*/

{{ config(materialized='table') }}

select
    row_number() over(order by violation_code) as violation_id,
    violation_code,
    violation_description,
    critical_flag,
    action
from 
(select
      distinct violation_code,
      violation_description,
      critical_flag,
      action
from {{ ref('stg_nyc_inspect') }}
    )

