{{
    config(
        materialized='view'
    )
}}

WITH tripdata AS (
  SELECT *,
    ROW_NUMBER() OVER(PARTITION BY vendorid, lpep_pickup_datetime) AS rn
  FROM {{ source('staging', 'green_tripdata') }}
  WHERE vendorid IS NOT NULL 
),
cleaned_data AS (
  SELECT
    -- Identifiers
    {{ dbt_utils.generate_surrogate_key(['vendorid', 'lpep_pickup_datetime']) }} as tripid,
    cast(vendorid as integer) as vendorid,
    cast(ratecodeid as integer) as ratecodeid,
    cast(pulocationid as integer) as pickup_locationid,
    cast(dolocationid as integer) as dropoff_locationid,
    -- timestamps
    cast(lpep_pickup_datetime as timestamp) as pickup_datetime,
    cast(lpep_dropoff_datetime as timestamp) as dropoff_datetime,

    -- Trip info
    store_and_fwd_flag,
    {{ dbt.safe_cast("passenger_count", api.Column.translate_type("integer")) }} AS passenger_count,
    CAST(trip_distance AS NUMERIC) AS trip_distance,
    1 AS trip_type,
    -- Payment info
    CAST(fare_amount AS NUMERIC) AS fare_amount,
    CAST(extra AS NUMERIC) AS extra,
    CAST(mta_tax AS NUMERIC) AS mta_tax,
    CAST(tip_amount AS NUMERIC) AS tip_amount,
    CAST(tolls_amount AS NUMERIC) AS tolls_amount,
    CAST(0 AS NUMERIC) AS ehail_fee,
    CAST(improvement_surcharge AS NUMERIC) AS improvement_surcharge,
    CAST(total_amount AS NUMERIC) AS total_amount,
    -- Cleaned payment type
    CASE
    WHEN REGEXP_CONTAINS(CAST(payment_type AS STRING), r'^[0-9]+(\.0)?$') THEN  
        SAFE_CAST(REPLACE(CAST(payment_type AS STRING), '.0', '') AS INT64) 
    ELSE NULL  
END AS payment_type
  FROM tripdata
  WHERE rn = 1
)
SELECT 
    *,
    {{ get_payment_type_description('payment_type') }} AS payment_type_description
FROM cleaned_data  

-- -- 
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}

