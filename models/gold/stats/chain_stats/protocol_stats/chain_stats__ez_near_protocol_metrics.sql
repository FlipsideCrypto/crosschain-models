{{ config(
    materialized = 'view',
    tags = ['metrics_daily']
) }}
{{ get_protocol_metrics_daily('near') }}
