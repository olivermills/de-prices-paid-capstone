SELECT      count(1)
            ,year_of_transfer
FROM        `spatial-thinker-384120.pp_bq_dataset.external_pp_data`
group by    year_of_transfer
order by    year_of_transfer

SELECT      count(1)
            ,year_of_transfer
FROM        `spatial-thinker-384120.pp_bq_dataset.pp_data_partitioned_clustered`
group by    year_of_transfer
order by    year_of_transfer

SELECT      count(1)
FROM        `spatial-thinker-384120.pp_bq_dataset.external_pp_data`
where       transaction_unique_identifier is null
or          date_of_transfer is null
or          price is null
or          county is null

SELECT      count(1)
FROM        `spatial-thinker-384120.pp_bq_dataset.pp_data_partitioned_clustered`
where       transaction_unique_identifier is null
or          date_of_transfer is null
or          price is null
or          county is null