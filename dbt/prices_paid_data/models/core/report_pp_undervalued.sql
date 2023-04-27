{{ config(materialized='table') }}

-- Query to identify undervalued towns/cities
with cteAvgPriceByPropType as
        (
          select    PPD.property_type,
                    avg(PPD.price) AS property_type_price
          from      {{ ref('stg_pp_data') }} as PPD
          group by  PPD.property_type
        )

select    PPD.town_or_city,
          avg(PPD.price)                                as average_price,
          avg(PPD.price) / avg(CTE.property_type_price) as price_ratio
from      {{ ref('stg_pp_data') }} as PPD
join      cteAvgPriceByPropType    as CTE ON PPD.property_type = CTE.property_type
group by  PPD.town_or_city, 
          CTE.property_type, 
          CTE.property_type_price
having    price_ratio < 1
order by  price_ratio asc