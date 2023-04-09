/*
    Creates a dimension table for name
*/

{{ config(materialized='table') }}

select
    row_number() over(order by cuisine_description) as cuisine_id,
    cuisine_description
from
(    select
      distinct
      cuisine_description,
    from {{ ref('stg_nyc_inspect') }}
     )

