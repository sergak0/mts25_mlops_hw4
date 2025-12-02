{% macro amount_bucket(amount_column) %}
    case
        when {{ amount_column }} < 50 then 'LOW'
        when {{ amount_column }} < 200 then 'MEDIUM'
        else 'HIGH'
    end
{% endmacro %}
