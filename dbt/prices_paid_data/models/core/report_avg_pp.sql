{{ config(materialized='table') }}

--Query to find average price per month and year
select    avg(PPD.price)                            as average_price
          ,date_trunc(PPD.date_of_transfer, month)  as month_of_transfer
          ,date_trunc(PPD.date_of_transfer, year)   as year_of_transfer
from      {{ ref('stg_pp_data') }} as PPD
group by  month_of_transfer
          ,year_of_transfer
order by  month_of_transfer
          ,year_of_transfer