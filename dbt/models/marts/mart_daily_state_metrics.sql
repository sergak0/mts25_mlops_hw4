{{ config(materialized='table') }}

with tx as (
    select *
    from {{ ref('stg_transactions') }}
)

select
    transaction_date,
    us_state,
    count() as tx_count,
    sum(amount) as total_amount,
    avg(amount) as avg_amount,
    quantile(0.95)(amount) as p95_amount,
    avg(if(amount > 200, 1.0, 0.0)) as high_amount_share_pct,
    {{ dbt_date.week_start('transaction_date') }} as week_start_date
from tx
group by
    transaction_date,
    us_state
