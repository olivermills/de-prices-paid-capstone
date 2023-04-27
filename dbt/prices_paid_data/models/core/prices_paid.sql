{{ config(materialized='table') }}

select  transaction_unique_identifier
        ,price
        ,date_of_transfer
        ,year_of_transfer
        ,postcode
        ,REGEXP_EXTRACT(postcode, r'^([A-Z]+)') AS postcode_area
        ,CASE 
            WHEN property_type = 'D' THEN 'Detached'
            WHEN property_type = 'S' THEN 'Semi-Detached'
            WHEN property_type = 'T' THEN 'Terraced'
            WHEN property_type = 'F' THEN 'Flats/Maisonettes'
            ELSE 'Other'
        END AS property_type
        ,CASE
            WHEN old_or_new = 'N' THEN 'Old'
            WHEN old_or_new = 'Y' THEN 'New'
            ELSE null
        END as old_or_new
        ,CASE
            WHEN duration = 'F' THEN 'Freehold'
            WHEN duration = 'L' THEN 'Leasehold'
            ELSE null
        END as duration
        ,CASE 
            WHEN price BETWEEN 1        AND 250000      THEN '0-250k'
            WHEN price BETWEEN 250001   AND 500000      THEN '250k-500k'
            WHEN price BETWEEN 500001   AND 1000000     THEN '500k-1m'
            WHEN price BETWEEN 1000001  AND 2000000     THEN '1m-2m'
            WHEN price BETWEEN 2000001  AND 3000000     THEN '2m-3m'
            WHEN price BETWEEN 3000001  AND 4000000     THEN '3m-4m'
            WHEN price BETWEEN 4000001  AND 5000000     THEN '4m-5m'
            WHEN price BETWEEN 5000001  AND 10000000    THEN '5m-10m'
            WHEN price BETWEEN 10000001 AND 50000000    THEN '10m-50m'
            ELSE '50m+' 
        END AS price_range
        ,CASE 
            WHEN price BETWEEN 1        AND 250000      THEN 1
            WHEN price BETWEEN 250001   AND 500000      THEN 2
            WHEN price BETWEEN 500001   AND 1000000     THEN 3
            WHEN price BETWEEN 1000001  AND 2000000     THEN 4
            WHEN price BETWEEN 2000001  AND 3000000     THEN 5
            WHEN price BETWEEN 3000001  AND 4000000     THEN 6
            WHEN price BETWEEN 4000001  AND 5000000     THEN 7
            WHEN price BETWEEN 5000001  AND 10000000    THEN 8
            WHEN price BETWEEN 10000001 AND 50000000    THEN 9
            ELSE 10
        END AS price_range_index
        ,CASE
            WHEN ppd_category_type = 'A' THEN 'Standard Price Paid'
            WHEN ppd_category_type = 'B' THEN 'Additional Price Paid'
            ELSE null
        end as ppd_category_type
        ,case
            when record_status = 'A' then 'Addition'
            when record_status = 'B' then 'Change'
            when record_status = 'C' then 'Delete'
            else null
        end as record_status
        ,primary_addressable_object_name
        ,secondary_addressable_object_name
        ,street
        ,locality
        ,town_or_city
        ,district
        ,county
from    {{ ref('stg_pp_data') }}