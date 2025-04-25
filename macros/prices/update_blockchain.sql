{% macro update_blockchain(new_blockchain_name, current_blockchain_names) %}

{% for table in [
    'crosschain.silver.token_prices_all_providers',
    'crosschain.silver.token_asset_metadata_all_providers',
    'crosschain.silver.token_asset_metadata_priority',
    'crosschain.silver.token_prices_priority',
    'crosschain.silver.complete_token_asset_metadata',
    'crosschain.silver.complete_token_prices'
] %}

{% set sql %}
UPDATE {{ table }}
SET blockchain = '{{ new_blockchain_name }}'
WHERE blockchain IN ({{ current_blockchain_names | join(", ") }});
{% endset %}

{% do run_query(sql) %}

{% endfor %}

{% endmacro %}
