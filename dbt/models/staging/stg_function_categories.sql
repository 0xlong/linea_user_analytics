-- Categorizes transaction functions into activity buckets
-- Used for user behavior analysis and segmentation

WITH categorized AS (
    SELECT
        transaction_hash,
        from_address AS user_address,
        block_timestamp,
        TO_CHAR(block_timestamp, 'YYYY-MM') AS activity_month,
        DATE_TRUNC('hour', block_timestamp)::DATE AS activity_hour,
        function_name,
        CASE
            -- Bridging: Cross-chain asset transfers
            WHEN LOWER(function_name) LIKE '%bridge%'
                OR LOWER(function_name) LIKE 'depositeth(%'
                OR LOWER(function_name) LIKE 'withdraweth(%'
                OR LOWER(function_name) LIKE 'sendmessage(%'
                OR LOWER(function_name) LIKE 'send(%'
                OR LOWER(function_name) LIKE 'transferremote(%'
                OR LOWER(function_name) LIKE 'depositforburn(%'
                OR LOWER(function_name) LIKE 'receivemessage(%'
                OR LOWER(function_name) LIKE 'sendfrom(%'
                OR LOWER(function_name) LIKE 'sendtoken(%'
                THEN 'Bridging'

            -- Swapping: DEX and aggregator trades
            WHEN LOWER(function_name) LIKE 'swap%'
                OR LOWER(function_name) LIKE '%swap(%'
                OR LOWER(function_name) LIKE 'execute(bytes commands%'
                OR LOWER(function_name) LIKE 'exactinput%'
                OR LOWER(function_name) LIKE 'exactoutput%'
                OR LOWER(function_name) LIKE 'smartswap%'
                OR LOWER(function_name) LIKE 'multicall(%'
                OR LOWER(function_name) LIKE 'snwap%'
                THEN 'Swapping'

            -- NFT: Minting, buying, selling NFTs
            WHEN LOWER(function_name) LIKE 'mint%'
                OR LOWER(function_name) LIKE 'safemint%'
                OR LOWER(function_name) LIKE '%erc721%'
                OR LOWER(function_name) LIKE '%erc1155%'
                OR LOWER(function_name) LIKE 'mintcube%'
                OR LOWER(function_name) LIKE 'mintwithvoucher%'
                OR LOWER(function_name) LIKE 'mintwithsignature%'
                OR LOWER(function_name) LIKE 'purchase(%'
                THEN 'NFT'

            -- DeFi: Lending, liquidity, staking
            WHEN LOWER(function_name) LIKE 'supply(%'
                OR LOWER(function_name) LIKE 'borrow(%'
                OR LOWER(function_name) LIKE 'repay%'
                OR LOWER(function_name) LIKE 'addliquidity%'
                OR LOWER(function_name) LIKE 'removeliquidity%'
                OR LOWER(function_name) LIKE 'stake(%'
                OR LOWER(function_name) LIKE 'unstake%'
                OR LOWER(function_name) LIKE 'deposit(%'
                OR LOWER(function_name) LIKE 'withdraw(%'
                OR LOWER(function_name) LIKE 'harvest%'
                OR LOWER(function_name) LIKE 'getreward%'
                OR LOWER(function_name) LIKE 'claimrewards%'
                OR LOWER(function_name) LIKE 'redeem%'
                OR LOWER(function_name) LIKE 'createlock%'
                OR LOWER(function_name) LIKE 'increaseliquidity%'
                THEN 'DeFi'

            -- Identity & Governance: Attestations, voting, quests
            WHEN LOWER(function_name) LIKE 'checkin%'
                OR LOWER(function_name) LIKE 'attest%'
                OR LOWER(function_name) LIKE 'vote(%'
                OR LOWER(function_name) LIKE 'register%'
                OR LOWER(function_name) LIKE 'participate%'
                OR LOWER(function_name) LIKE 'verifyandattest%'
                OR LOWER(function_name) LIKE 'setscore%'
                OR LOWER(function_name) LIKE 'onchaingm%'
                OR LOWER(function_name) LIKE 'dailygm%'
                THEN 'Identity_Governance'

            -- Infrastructure: Token approvals, transfers
            WHEN LOWER(function_name) LIKE 'approve(%'
                OR LOWER(function_name) LIKE 'transfer(%'
                OR LOWER(function_name) LIKE 'setapproval%'
                OR LOWER(function_name) LIKE 'permit(%'
                OR LOWER(function_name) LIKE 'approvedelegation%'
                THEN 'Infrastructure'

            -- Claim: Generic claims (could be rewards, airdrops, etc.)
            WHEN LOWER(function_name) LIKE 'claim(%'
                OR LOWER(function_name) LIKE 'claimall%'
                THEN 'Claims'

            -- NULL or unrecognized functions
            WHEN function_name IS NULL
                THEN 'Unknown'

            ELSE 'Unclassified'
        END AS activity_bucket
    FROM {{ ref('stg_linea_transactions') }}
)

SELECT * FROM categorized
