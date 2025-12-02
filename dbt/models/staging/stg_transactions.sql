{{ config(materialized='view') }}

select
    cast(transaction_time as DateTime) as transaction_ts,
    toDate(transaction_time) as transaction_date,
    toHour(transaction_time) as transaction_hour,
    toDayOfWeek(transaction_time) as transaction_dow,

    merch,
    cat_id,
    cast(amount as Float64) as amount,
    name_1,
    name_2,
    gender,
    street,
    one_city,
    us_state,
    post_code,
    cast(lat as Float64) as lat,
    cast(lon as Float64) as lon,
    cast(population_city as UInt32) as population_city,
    jobs,
    cast(merchant_lat as Float64) as merchant_lat,
    cast(merchant_lon as Float64) as merchant_lon,
    cast(target as UInt8) as target,

    {{ amount_bucket('amount') }} as amount_bucket,
    (cast(target as UInt8) = 1) as is_fraud
from {{ source('transactions_db', 'transactions') }}
