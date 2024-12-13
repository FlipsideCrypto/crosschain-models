{% docs bridge_ez_table_doc %}

A comprehensive convenience table holding blockchain and platform specific bridge activity from event_logs, traces and transfers, including bridge deposits/withdrawals and transfers in/out on the following blockchains: Arbitrum, Avalanche, Base, Blast,BSC, Gnosis, Ethereum, Optimism, Polygon, Solana, Aptos, Near, Flow and more. This table also includes prices and token symbols, where available. Note: source_address and destination_address are intentionally case-sensitive where applicable, depending on the requirements of the network.

{% enddocs %}

{% docs bridge_fact_table_doc %}

A comprehensive fact table holding blockchain and platform specific bridge activity from event_logs, traces and transfers, including bridge deposits/withdrawals and transfers in/out on the following blockchains: Arbitrum, Avalanche, Base, BSC, Gnosis, Ethereum, Optimism, Polygon, and Solana.

{% enddocs %}

{% docs bridge_platform %}

The platform or protocol from which the bridge transaction or event originates.

{% enddocs %}

{% docs bridge_sender %}

The address that initiated the bridge deposit or transfer. This address is the sender of the tokens/assets being bridged to the destination chain. This may be an EOA or contract address.

{% enddocs %}

{% docs bridge_destination_chain_receiver %}

The designated address set to receive the bridged tokens on the target chain after the completion of the bridge transaction. For non-evm chains, the hex address is decoded/encoded to match the data format of the destination chain, where possible. This may be an EOA or contract address.

{% enddocs %}

{% docs bridge_source_chain %}

The name of the blockchain network to which the assets are being bridged from.

{% enddocs %}

{% docs bridge_destination_chain %}

The name of the blockchain network to which the assets are being bridged to.

{% enddocs %}

{% docs bridge_direction %}

Indicates the direction in which the assets are being bridged, out/outbound or in/inbound.

{% enddocs %}

{% docs bridge_address %}

The address of the contract responsible for handling the bridge deposit or transfer. This contract mediates the transfer and ensures that assets are sent and received appropriately.

{% enddocs %}

{% docs bridge_token_address %}

The address associated with the token that is being bridged. It provides a unique identifier for the token within its origin blockchain.

{% enddocs %}

{% docs bridge_token_symbol %}

The symbol representing the token being bridged. This provides a shorthand representation of the token.

{% enddocs %}

{% docs bridge_amount_unadj %}

The raw, non-decimal adjusted amount of tokens involved in the bridge transaction. For Solana, these are decimal adjusted amounts.

{% enddocs %}

{% docs bridge_amount %}

The decimal adjusted amount of tokens involved in the bridge transaction, where available.

{% enddocs %}

{% docs bridge_amount_usd %}

The value of the bridged tokens in USD at the time of the bridge transaction, where available.

{% enddocs %}