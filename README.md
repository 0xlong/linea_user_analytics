# Linea User Analytics 🚀

> **Cohort retention and behavior analysis for users bridging from Ethereum Mainnet to Linea**  

[screen-capture (1).webm](https://github.com/user-attachments/assets/6f3ae5ad-9324-4d2e-8c65-f75314b6bf81)


## Overview

This project implements a full-stack data engineering pipeline to answer the critical business question:

> **"Are users who bridge to Linea stay or just passing through?"**

We track users bridging ETH from Ethereum Mainnet via the Linea Canonical Bridge, then analyze their ongoing activity on Linea to build cohort retention matrices—segmented by user transaction size (Whales 🐋 vs Retail 🐟), transaction number, timezone, activity, etc.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        EXTRACTION LAYER                              │
├─────────────────────────────────────────────────────────────────────┤
│   Etherscan API                        Lineascan API                │
│        ↓                                    ↓                       │
│   Bridge Logs (MessageSent)          User Transactions              │
│        ↓                                    ↓                       │
│   data/raw/                           data/raw/                     │
└─────────────────────────────────────────────────────────────────────┘
                                  ↓
┌─────────────────────────────────────────────────────────────────────┐
│                        LOADING LAYER                                 │
├─────────────────────────────────────────────────────────────────────┤
│                         PostgreSQL                                  │
└─────────────────────────────────────────────────────────────────────┘
                                  ↓
┌─────────────────────────────────────────────────────────────────────┐
│                     TRANSFORMATION LAYER (dbt)                       │
├─────────────────────────────────────────────────────────────────────┤
│   staging/              intermediate/             marts/            │
│   └─ Raw → Clean        └─ Business Logic         └─ Analytics     │
│      stg_etherscan         int_user_cohorts          dim_users     │
│      stg_linea_txs         int_user_segments         fct_retention │
│      stg_functions         int_monthly_activity                     │
└─────────────────────────────────────────────────────────────────────┘
                                  ↓
┌─────────────────────────────────────────────────────────────────────┐
│                      PRESENTATION LAYER                              │
├─────────────────────────────────────────────────────────────────────┤
│             Looker / Tableau / Preset Dashboard                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Layer | Technology | Purpose |
|-------|------------|---------|
| **Extraction** | Python 3.10+ + Etherscan/Lineascan APIs | Pull on-chain data |
| **Storage** | PostgreSQL | Data warehouse |
| **Transformation** | dbt (data build tool) | SQL modeling & business logic |
| **Visualization** | Superset / Preset | Dashboards |
| **Orchestration** | Airflow (Planned) | Pipeline execution |

---

## Data Sources

### 1. Bridge Deposits (Ethereum Mainnet)

**Contract:** `0xd19d4B5d358258f05D7B411E21A1460D11B0876F` (Linea Canonical Bridge)

**Event:** `MessageSent` — captures every bridge transaction from L1 → Linea

| Field | Description |
|-------|-------------|
| `_from` | User wallet address (our user ID) |
| `_to` | Destination on Linea |
| `_value` | Amount bridged (wei) |
| `_fee` | Bridge fee paid |

### 2. Linea Activity (Linea Mainnet)

Tracking behavior of users who bridged to Linea. 

---

## Project Structure

```
linea_user_analytics/
│
├── src/                                                # Python source code
│   ├── extract/                                        # Data extraction scripts
│   │   ├── extract_logs_from_etherscan.py
│   │   └── extract_transactions_from_lineascan.py
│   ├── load/                                           # Database loading
│   └── utils/                                          # Shared utilities & config
│
├── dbt/                                                # dbt project
│   ├── models/
│   │   ├── staging/                                    # Raw → Clean transformations
│   │   ├── intermediate/                               # Business logic layer
│   │   └── marts/                                      # Analytics-ready tables
│   ├── seeds/                                          # Reference data
│   └── tests/                                          # Data quality tests
│
├── data/                                               # Raw & processed data (gitignored)
├── project_context/                                    # Documentation & architecture
└── tests/                                              # Python unit tests
```

---

## Key dbt Models

### Staging Layer
| Model | Description |
|-------|-------------|
| `stg_etherscan_logs` | Cleaned bridge deposit events |
| `stg_linea_transactions` | Cleaned Linea activity |
| `stg_function_categories` | Function/method classifications into activity buckets |

### Intermediate Layer
| Model | Description |
|-------|-------------|
| `int_user_cohorts` | Assigns users to cohorts based on first bridge date |
| `int_user_segments` | Classifies Whale vs Retail by bridge volume |
| `int_monthly_activity` | Aggregates activity by user-month |

### Marts Layer
| Model | Description |
|-------|-------------|
| `dim_users` | Master user dimension with segment, cohort, and engagement status |
| `fct_cohort_retention` | Core retention matrix (cohort × relative month). Includes activation rate, retention rate, and cumulative retention. |
| `mart_activity_buckets` | Activity distribution analysis by bucket type |
| `mart_wallet_trading_sessions` | Trading session analysis (Asian/European/American) based on transaction UTC hour |

---

## Activity Buckets

User on-chain activity is classified into **8 distinct buckets** based on transaction function signatures. This classification powers behavioral analysis and helps understand what users are actually doing on Linea.

| Bucket | Description | Function Name Patterns |
|--------|-------------|------------------------|
| **Bridging** | Cross-chain asset transfers | `bridge*`, `depositETH`, `withdrawETH`, `sendMessage`, `transferRemote`, `depositForBurn` |
| **Swapping** | DEX and aggregator trades | `swap*`, `exactInput*`, `exactOutput*`, `multicall`, `approve`, `permit` |
| **NFT** | Minting, buying, selling NFTs | `mint*`, `safeMint*`, `erc721*`, `erc1155*`, `mintWithVoucher`, `purchase` |
| **DeFi** | Lending, liquidity, staking | `supply`, `borrow`, `repay`, `addLiquidity`, `stake`, `deposit`, `withdraw`, `harvest` |
| **DAO_and_Campaigns** | Identity, governance, quests | `checkin*`, `attest*`, `vote`, `register`, `participate`, `onChainGM`, `dailyGM` |
| **Transfers** | Direct token sends | `transfer(*)` |
| **Claims** | Reward/airdrop claims | `claim(*)`, `claimAll*` |
| **Unknown/Unclassified** | NULL or unrecognized functions | — |

> **Source**: [`stg_function_categories.sql`](dbt/models/staging/stg_function_categories.sql)

---

## Metric Construction in dbt

This section documents how key metrics are constructed in the dbt transformation layer.

### 1. Cohort Assignment (`int_user_cohorts`)

A **cohort** is defined as the month when a user first bridged from Ethereum to Linea.

```sql
-- Cohort = MIN(block_timestamp) of bridge deposits grouped by user
cohort_month = TO_CHAR(MIN(block_timestamp), 'YYYY-MM')
```

**Output columns:**
- `user_address` — Wallet address (user ID)
- `first_bridge_timestamp` / `first_bridge_date` — When user first bridged
- `cohort_month` — YYYY-MM format cohort identifier
- `total_bridge_count` — Number of bridge transactions
- `total_bridged_eth` — Sum of bridged ETH

---

### 2. User Segmentation (`int_user_segments`)

Users are classified by **total bridge volume**:

| Segment | Threshold | Description |
|---------|-----------|-------------|
| **Whale** 🐋 | >10 ETH | Serious DeFi users |
| **Retail** 🐟 | ≤10 ETH | Casual users |

**Granular Tiers:**
| Tier | Range |
|------|-------|
| Mega Whale | >100 ETH |
| Whale | 10-100 ETH |
| Mid-tier | 1-10 ETH |
| Retail | 0.1-1 ETH |
| Micro | <0.1 ETH |

```sql
user_segment = CASE WHEN total_bridged_eth > 10 THEN 'Whale' ELSE 'Retail' END
```

---

### 3. Monthly Activity Tracking (`int_monthly_activity`)

Aggregates user transactions on Linea network per month:

| Metric | Definition |
|--------|------------|
| `transaction_count` | Total transactions in month |
| `active_days` | COUNT(DISTINCT date) of activity |
| `unique_contracts_interacted` | COUNT(DISTINCT to_address) |
| `total_eth_transacted` | SUM(value_eth) |
| `successful_tx_count` / `failed_tx_count` | Success/failure breakdown |

**Activity Level Classification:**
| Level | Threshold |
|-------|-----------|
| Power User | ≥50 transactions/month |
| Active | ≥10 transactions/month |
| Casual | ≥3 transactions/month |
| Minimal | <3 transactions/month |

---

### 4. Retention Rate Calculation (`fct_cohort_retention`)

The retention matrix is calculated using a **relative month** approach:

```
months_since_bridge = (activity_year×12 + activity_month) - (cohort_year×12 + cohort_month)
```

**Three retention metrics are provided:**

| Metric | Formula | Use Case |
|--------|---------|----------|
| **Activation Rate** | `non_bridge_users / cohort_size` (Month 0 only) | True engagement beyond just bridging |
| **Retention Rate** | `active_users / cohort_size` | Monthly engagement rate |
| **Cumulative Retention** | Users active in ALL months 0→N | Strictly decreasing "SaaS-style" retention |

> **Key insight**: Month 0 uses "Activation Rate" (excludes bridge transaction) to measure true post-bridge engagement.

---

### 5. Churn Detection (`dim_users`)

A user is flagged as **churned** if:
```sql
is_churned = CASE
    WHEN last_active_month IS NULL THEN TRUE  -- Never active on Linea
    WHEN last_active_month < TO_CHAR(CURRENT_DATE - INTERVAL '2 months', 'YYYY-MM') THEN TRUE
    ELSE FALSE
END
```

---

### 6. Engagement Status (`dim_users`)

Users are classified into lifecycle stages:

| Status | Criteria |
|--------|----------|
| **High Value Retained** | 3+ active months AND Whale segment |
| **Retained** | 3+ active months |
| **Engaged** | 1-2 active months |
| **Bridge Only** | 0 active months on Linea |

---

### 7. Trading Session Analysis (`mart_wallet_trading_sessions`)

Transactions are classified by UTC hour into trading sessions:

| Session | UTC Hours | Markets |
|---------|-----------|---------|
| Asian | 00:00 - 08:00 | Binance, OKX |
| European | 08:00 - 16:00 | Kraken, Bitstamp |
| American | 13:00 - 22:00 | Coinbase, Gemini |
| Off Hours | 22:00 - 00:00 | — |

**Dominant session** = session with the most transactions per user.

---

## Quick Start

### Prerequisites
- Python 3.10+
- PostgreSQL database running
- API Keys for [Etherscan](https://etherscan.io/apis) and [Lineascan](https://lineascan.build/apis)

### 1. Environment Setup

```bash
git clone <repo-url> && cd linea_user_analytics
python -m venv linea_user_analytics_env && .\linea_user_analytics_env\Scripts\activate
pip install -r requirements.txt
```

Create `.env` file with your credentials:

```env
ETHERSCAN_API_KEY=your_key
LINEASCAN_API_KEY=your_key
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=linea_analytics
POSTGRES_USER=your_user
POSTGRES_PASSWORD=your_password
```

### 2. Extract Data

```bash
python src/extract/extract_logs_from_etherscan.py ; python src/extract/extract_transactions_from_lineascan.py
```

### 3. Load to Database

```bash
python src/load/load_to_postgres.py
```

### 4. Run dbt Transformations

```bash
cd dbt ; dbt deps ; dbt build
```

### 5. Verify Results

```bash
dbt test
```

---

## Configuration

Key settings in `src/utils/config.py`:

| Parameter | Description |
|-----------|-------------|
| `LINEA_BRIDGE_CONTRACT` | Canonical bridge contract address |
| `MESSAGE_SENT_TOPIC` | Event topic for bridge deposits |
| `EXTRACTION_START_DATE` | Analysis start date |
| `EXTRACTION_END_DATE` | Analysis end date |
| `WHALE_THRESHOLD_ETH` | ETH threshold for Whale classification (default: 10) |

---

## API Endpoints

```
Ethereum:  https://api.etherscan.io/api
Linea:     https://api.lineascan.build/api
Infura:    https://mainnet.infura.io/v3/{api_key}
           https://linea-mainnet.infura.io/v3/{api_key}
```
