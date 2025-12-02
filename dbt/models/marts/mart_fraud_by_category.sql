{{ config(materialized='table') }}

with tx as (
    select *
    from {{ ref('stg_transactions') }}
),

agg as (
    select
        cat_id,
        count() as tx_count,
        sum(if(is_fraud, 1, 0)) as fraud_tx_count,
        sum(amount) as total_amount,
        sum(if(is_fraud, amount, 0)) as fraud_amount,
        100.0 * fraud_tx_count / nullIf(tx_count, 0) as fraud_rate
    from tx
    group by
        cat_id
)

select *
from agg
