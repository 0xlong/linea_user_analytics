
with source as (


    select * from {{ source('linea_analytics', 'etherscan_logs') }}
    where datetime >= '2025-07-31' and datetime <= '2026-01-27' --temp fix until new data come

),

renamed as (

    select
        -- Identifiers
        tx_hash as transaction_hash,
        block_number,
        log_index, -- index of the log in the block
        message_hash as unique_message_hash, -- we got this info extracted during transofmration process

        -- Timestamps
        datetime as block_timestamp, -- 'datetime' is vague, 'block_timestamp' is standard
        timestamp as block_timestamp_unix,

        -- Transaction Details
        from_address,
        to_address,
        value_eth,
        fee_eth,
        
        -- Gas / Tech details
        gas_price,
        gas_used,
        nonce,
        
        -- Metadata
        loaded_at -- when the data was loaded into the database

    from source

)

select * from renamed
