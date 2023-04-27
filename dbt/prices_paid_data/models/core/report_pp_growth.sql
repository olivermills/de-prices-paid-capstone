{{ config(materialized='table') }}

--Query to find counties that have experienced strong price growth
WITH prices_by_county AS 
    (
      SELECT    county
                ,EXTRACT(YEAR FROM date_of_transfer)  AS `Year`
                ,AVG(`Price`)                         AS `Average Price`
      FROM      {{ ref('stg_pp_data') }}
      GROUP BY  county
                ,`Year`
    )

SELECT    county
          ,(`2022 Average Price` - `2016 Average Price`) / `2016 Average Price` AS price_increase
FROM      (
            SELECT    `County` 
                      ,AVG(IF(`Year` = 2016, `Average Price`, NULL)) AS `2016 Average Price` 
                      ,AVG(IF(`Year` = 2022, `Average Price`, NULL)) AS `2022 Average Price`
            FROM      prices_by_county
            GROUP BY  `County`
          )
ORDER BY price_increase DESC