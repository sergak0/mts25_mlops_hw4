{{ config(materialized='table') }}

with tx as (
    select *
    from {{ ref('stg_transactions') }}
),

agg as (
    select
        us_state,
        count() as tx_count,
        sum(if(is_fraud, 1, 0)) as fraud_tx_count,
        uniqExact(concat(name_1, name_2)) as unique_customers,
        uniqExact(merch) as unique_merchants,
        sum(amount) as total_amount,
        sum(if(is_fraud, amount, 0)) as fraud_amount,
        100.0 * fraud_tx_count / nullIf(tx_count, 0) as fraud_rate
    from tx
    group by
        us_state
)

select *
from agg
