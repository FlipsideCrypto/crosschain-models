{% docs prices_dim_asset_metadata_table_doc %}

A comprehensive dimensional table holding asset metadata and other relevant details pertaining to each id, from multiple providers. This data set includes raw, non-transformed data coming directly from the provider APIs and rows are not intended to be unique. As a result, there may be data quality issues persisting in the APIs that flow through to this dimensional model. If you are interested in using a curated data set instead, please utilize ez_asset_metadata.

{% enddocs %}

{% docs prices_ez_asset_metadata_table_doc %}

A convenience table holding prioritized asset metadata and other relevant details pertaining to each token_address and native asset. This data set is highly curated and contains metadata for one unique asset per blockchain.

{% enddocs %}

{% docs prices_fact_prices_ohlc_hourly_table_doc %}

A comprehensive fact table holding id and provider specific open, high, low, close hourly prices, from multiple providers. This data set includes raw, non-transformed data coming directly from the provider APIs and rows are not intended to be unique. As a result, there may be data quality issues persisting in the APIs that flow through to this fact based model. If you are interested in using a curated data set instead, please utilize ez_prices_hourly.

{% enddocs %}

{% docs prices_ez_prices_hourly_table_doc %}

A convenience table for determining token prices by address and blockchain, and native asset prices by symbol and blockchain. This data set is highly curated and contains metadata for one price per hour per unique asset and blockchain.

{% enddocs %}

{% docs prices_fact_asset_metrics_daily_table_doc %}

A comprehensive fact table containing daily asset fundamentals including current pricing, supply metrics, market capitalization, historical extremes, and trading volume from provider APIs. This dataset provides a complete daily snapshot of key financial and market metrics for each asset, enabling fundamental analysis and performance tracking over time.

{% enddocs %}

{% docs prices_provider %}

The provider or source of the data.

{% enddocs %}

{% docs prices_asset_id %}

The unique identifier representing the asset.

{% enddocs %}

{% docs prices_name %}

The name of asset.

{% enddocs %}

{% docs prices_symbol %}

The symbol of asset.

{% enddocs %}

{% docs prices_token_address %}

The specific address representing the asset on a specific platform. This will be NULL if referring to a native asset.

{% enddocs %}

{% docs prices_blockchain %}

The Blockchain, Network, or Platform for this asset.

{% enddocs %}

{% docs prices_blockchain_id %}

The unique identifier of the Blockchain, Network, or Platform for this asset.

{% enddocs %}

{% docs prices_decimals %}

The number of decimals for the asset. May be NULL.

{% enddocs %}

{% docs prices_is_native %}

A flag indicating assets native to the respective blockchain.

{% enddocs %}

{% docs prices_is_deprecated %}

A flag indicating if the asset is deprecated or no longer supported by the provider.

{% enddocs %}

{% docs prices_id_deprecation %}

Deprecating soon! Please use the `asset_id` column instead.

{% enddocs %}

{% docs prices_decimals_deprecation %}

Deprecating soon! Please use the decimals column in `ez_asset_metadata` or join in `dim_contracts` instead.

{% enddocs %}

{% docs prices_hour %}

Hour that the price was recorded at.

{% enddocs %}

{% docs prices_price %}

Closing price of the recorded hour in USD.

{% enddocs %}

{% docs prices_is_imputed %}

A flag indicating if the price was imputed, or derived, from the last arriving record. This is generally used for tokens with low-liquidity or inconsistent reporting.

{% enddocs %}

{% docs prices_open %}

Opening price of the recorded hour in USD.

{% enddocs %}

{% docs prices_high %}

Highest price of the recorded hour in USD

{% enddocs %}

{% docs prices_low %}

Lowest price of the recorded hour in USD

{% enddocs %}

{% docs prices_close %}

Closing price of the recorded hour in USD

{% enddocs %}

{% docs prices_is_verified %}

A boolean flag indicating if the token is verified by either uniswap labs or passing a threshold of transfers metrics on the chain.
 

{% enddocs %}

{% docs prices_fully_diluted_valuation %}

The total market value of the asset if all tokens were in circulation, calculated as current price multiplied by max supply.

{% enddocs %}

{% docs prices_circulating_supply %}

The total number of tokens currently in circulation and available for trading in the market.

{% enddocs %}

{% docs prices_total_supply %}

The total number of tokens that currently exist, including those held in reserve or locked.

{% enddocs %}

{% docs prices_max_supply %}

The maximum number of tokens that will ever be created for this asset, or NULL if no maximum is defined.

{% enddocs %}

{% docs prices_current_price %}

The current trading price of the asset in USD at the time of data collection.

{% enddocs %}

{% docs prices_ath %}

The highest price (All-Time High) that the asset has ever reached in USD.

{% enddocs %}

{% docs prices_ath_change_percentage %}

The percentage change from the current price to the all-time high price, typically shown as a negative value.

{% enddocs %}

{% docs prices_ath_date %}

The timestamp when the asset reached its all-time high price.

{% enddocs %}

{% docs prices_atl %}

The lowest price (All-Time Low) that the asset has ever reached in USD.

{% enddocs %}

{% docs prices_atl_change_percentage %}

The percentage change from the current price to the all-time low price, typically shown as a positive value.

{% enddocs %}

{% docs prices_atl_date %}

The timestamp when the asset reached its all-time low price.

{% enddocs %}

{% docs prices_high_24h %}

The highest price the asset reached in USD within the last 24 hours.

{% enddocs %}

{% docs prices_low_24h %}

The lowest price the asset reached in USD within the last 24 hours.

{% enddocs %}

{% docs prices_price_change_24h %}

The absolute change in price (in USD) over the last 24 hours.

{% enddocs %}

{% docs prices_price_change_percentage_24h %}

The percentage change in price over the last 24 hours.

{% enddocs %}

{% docs prices_market_cap %}

The total market value of the asset, calculated as current price multiplied by circulating supply.

{% enddocs %}

{% docs prices_market_cap_rank %}

The ranking of this asset by market capitalization compared to other assets tracked by the provider.

{% enddocs %}

{% docs prices_market_cap_change_24h %}

The absolute change in market capitalization (in USD) over the last 24 hours.

{% enddocs %}

{% docs prices_market_cap_change_percentage_24h %}

The percentage change in market capitalization over the last 24 hours.

{% enddocs %}

{% docs prices_total_volume %}

The total trading volume for the asset in USD over the last 24 hours across all exchanges.

{% enddocs %}

{% docs prices_roi_json %}

Return on investment data in JSON format, containing percentage and currency information for historical performance tracking.

{% enddocs %}

{% docs prices_image_url %}

The URL pointing to the official image or logo representing the asset.

{% enddocs %}

{% docs prices_last_updated %}

The timestamp when this market data record was last updated by the data provider.

{% enddocs %}