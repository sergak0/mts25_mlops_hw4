Версии dbt
dbt-core                  1.10.15
dbt-clickhouse            1.9.6

DAG
<img width="1280" height="650" alt="image" src="https://github.com/user-attachments/assets/7201a301-2e96-4696-9660-24e9c3b06d11" />

Пройденные тесты
<img width="1280" height="739" alt="image" src="https://github.com/user-attachments/assets/b676914d-e77f-42b2-8606-cc027eac9d33" />


Витрины 
Реализованы следующие аналитические витрины:
* mart_daily_state_metrics - дневные метрики по штатам: количество транзакций, сумма, средний чек, P95, доля крупных транзакций.
* mart_fraud_by_category - анализ фрода по категориям: fraud_rate, объёмы и количество мошеннических транзакций.
* mart_fraud_by_state - анализ фрода по штатам: fraud_rate, количество клиентов и мерчантов, суммы.
* mart_customer_risk_profile - профиль риска клиентов: сегментация в HIGH / MEDIUM / LOW.
* mart_hourly_fraud_pattern - временные паттерны фрода по часам и дням недели.
* mart_merchant_analytics - аналитика по мерчантам: fraud_rate, обороты и флаг подозрительности.

Тесты

Data tests (schema.yml)
* not_null, accepted_values
* dbt_utils.unique_combination_of_columns
* dbt_expectations.expect_column_values_to_be_between

Singular tests
* assert_no_negative_amounts - проверка отсутствия отрицательных значений в amount.
* assert_fraud_rate_bounds - проверка fraud_rate в диапазоне 0–100%.

Unit test
* mart_fraud_by_category_unit - проверка корректного расчёта tx_count и fraud_tx_count по категориям.
