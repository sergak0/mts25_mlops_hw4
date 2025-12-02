{{ config(materialized='table') }}

with tx as (
    select *
    from {{ ref('stg_transactions') }}
)

select
    transaction_dow,
    transaction_hour,
    count() as tx_count,
    sum(if(is_fraud, 1, 0)) as fraud_tx_count,
    100.0 * fraud_tx_count / nullIf(tx_count, 0) as fraud_rate
from tx
group by
    transaction_dow,
    transaction_hour
order by
    transaction_dow,
    transaction_hour
