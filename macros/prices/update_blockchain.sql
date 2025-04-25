{% macro update_blockchain(new_blockchain_name, current_blockchain_names) %}
{#
    This macro updates the blockchain column in the prices tables. 
    It requires a corresponding update in the all_providers tables to normalize the blockchain names.
    It takes two arguments:
    - new_blockchain_name: The new blockchain name to set in the blockchain column.
    - current_blockchain_names: A list of current blockchain names to update.
#}
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
