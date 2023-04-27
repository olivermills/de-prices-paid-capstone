/*
    Stage the prices paid dataset
*/

{{ config(materialized='view') }}

select   cast(transaction_unique_identifier	    as STRING   )	as transaction_unique_identifier	 		
        ,cast(price	                            as INTEGER	)	as price	                         	
        ,cast(date_of_transfer	                as date     )	as date_of_transfer	              		
        ,cast(postcode	                        as STRING	)	as postcode	                      	
        ,cast(property_type	                    as STRING	)	as property_type	                 	
        ,cast(old_or_new	                    as STRING	)	as old_or_new	                    	
        ,cast(duration	                        as STRING	)	as duration	                      	
        ,cast(primary_addressable_object_name	as STRING	)	as primary_addressable_object_name	
        ,cast(secondary_addressable_object_name	as STRING	)	as secondary_addressable_object_name	
        ,cast(street	                        as STRING	)	as street	                        	
        ,cast(locality	                        as STRING	)	as locality	                      	
        ,cast(town_or_city	                    as STRING	)	as town_or_city	                  	
        ,cast(district	                        as STRING	)	as district	                      	
        ,cast(county	                        as STRING	)	as county	                        	
        ,cast(ppd_category_type	                as STRING	)	as ppd_category_type	             	
        ,cast(record_status	                    as STRING	)	as record_status	                 	
        ,cast(year_of_transfer	                as INTEGER  )   as year_of_transfer	              
from    {{ source('staging', 'pp_data_partitioned_clustered') }}