{% macro create_tasks() %}
    {% if target.database == 'CROSSCHAIN' %}
        {{ task_run_sp_bulk_get_coin_market_cap_prices('resume') }};
        {{ task_run_sp_bulk_get_coin_gecko_prices('suspend') }};
    {% else %}
        {{ task_run_sp_bulk_get_coin_market_cap_prices('suspend') }};
        {{ task_run_sp_bulk_get_coin_gecko_prices('suspend') }};
    {% endif %}
{% endmacro %}