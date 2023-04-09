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
    phone,
    latitude,
    longitude,
    community_board,
    council_district,
    census_tract,
    bin,
    bbl,
    nta,
    dba
from (
    select
      distinct boro,
      building,
      street,
      zipcode,
      phone,
      latitude,
      longitude,
      community_board,
      council_district,
      census_tract,
      bin,
      bbl,
      nta,
      dba
   from {{ ref('stg_nyc_inspect') }}
    )
where boro <> '0'

