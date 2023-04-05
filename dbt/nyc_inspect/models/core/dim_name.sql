/*
    Creates a dimension table for name
*/

{{ config(materialized='table') }}

select
    row_number() over(order by cuisine_description) as name_id,
    camis,
    dba,
    cuisine_description,
    phone
from
(    select
      distinct camis,
      dba,
      cuisine_description,
      phone
    from {{ ref('stg_nyc_inspect') }}
     )

