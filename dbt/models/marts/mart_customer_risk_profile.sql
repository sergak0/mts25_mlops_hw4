{{ config(materialized='table') }}

with tx as (
    select *
    from {{ ref('stg_transactions') }}
),

per_customer as (
    select
        name_1,
        name_2,
        gender,
        us_state,
        count() as tx_count,
        sum(amount) as total_amount,
        avg(amount) as avg_amount,
        sum(if(is_fraud, 1, 0)) as fraud_tx_count,
        100.0 * fraud_tx_count / nullIf(tx_count, 0) as fraud_rate
    from tx
    group by
        name_1,
        name_2,
        gender,
        us_state
),

scored as (
    select
        name_1,
        name_2,
        gender,
        us_state,
        tx_count,
        total_amount,
        avg_amount,
        fraud_tx_count,
        fraud_rate,
        case
            when fraud_rate >= 10 then 'HIGH'
            when fraud_rate >= 3 then 'MEDIUM'
            else 'LOW'
        end as risk_bucket
    from per_customer
)

select *
from scored
