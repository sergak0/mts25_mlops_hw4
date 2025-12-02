{{ config(materialized='table') }}

with tx as (
    select *
    from {{ ref('stg_transactions') }}
),

per_merchant as (
    select
        merch,
        one_city,
        us_state,
        count() as tx_count,
        sum(amount) as total_amount,
        avg(amount) as avg_amount,
        sum(if(is_fraud, 1, 0)) as fraud_tx_count,
        sum(if(is_fraud, amount, 0)) as fraud_amount,
        uniqExact(concat(name_1, name_2)) as unique_customers,
        100.0 * fraud_tx_count / nullIf(tx_count, 0) as fraud_rate
    from tx
    group by
        merch,
        one_city,
        us_state
)

select
    merch,
    one_city,
    us_state,
    tx_count,
    total_amount,
    avg_amount,
    fraud_tx_count,
    fraud_amount,
    unique_customers,
    fraud_rate,
    case
        when fraud_rate >= 10 or fraud_tx_count >= 5 then 1
        else 0
    end as is_suspicious
from per_merchant
