
with source as (

    select * from {{ source('linea_analytics', 'linea_transactions') }}

),

renamed as (

    select
        -- Identifiers
        hash as transaction_hash,
        block_number,
        nonce_int as nonce,

        -- Timestamps
        datetime as block_timestamp,
        
        -- Transaction Details
        from_address,
        to_address,
        value_eth,
        
        -- Gas / Tech details
        gas_price_gwei,
        gas_used_int as gas_used,
        
        -- Status / Methods
        is_error as if_transaction_failed,
        tx_status,
        method_id,
        function_name,
        
        -- Metadata
        loaded_at

    from source

)

select * from renamed
