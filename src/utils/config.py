"""
Config - All project settings in one place
===========================================
"""

import os
from dotenv import load_dotenv

# Load .env file
load_dotenv()

# =============================================================================
# API KEYS (from .env file)
# =============================================================================

ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY")
LINEASCAN_API_KEY = os.getenv("LINEASCAN_API_KEY")
INFURA_API_KEY = os.getenv("INFURA_API_KEY")

# =============================================================================
# API URLS
# =============================================================================

# Block explorers (for fetching logs)
# Etherscan v2 API is unified - use chainid param to select network
ETHERSCAN_URL = "https://api.etherscan.io/v2/api"
LINEASCAN_URL = "https://api.etherscan.io/v2/api"  # Same v2 endpoint, use chainid=59144

# Chain IDs (required for v2 API)
ETHEREUM_CHAIN_ID = 1
LINEA_CHAIN_ID = 59144

# RPC endpoints (for direct blockchain queries)
ETHEREUM_RPC = f"https://mainnet.infura.io/v3/{INFURA_API_KEY}"
LINEA_RPC = f"https://linea-mainnet.infura.io/v3/{INFURA_API_KEY}"

# =============================================================================
# CONTRACT ADDRESSES
# =============================================================================

# Linea Canonical Bridge on Ethereum Mainnet
LINEA_BRIDGE_CONTRACT = "0xd19d4B5d358258f05D7B411E21A1460D11B0876F"

# =============================================================================
# EVENT SIGNATURES (Topic0)
# =============================================================================

# MessageSent event - fires when someone bridges ETH to Linea
# This is the keccak256 hash of: MessageSent(address,address,uint256,uint256,uint256,bytes,bytes32)
MESSAGE_SENT_TOPIC = "0xe856c2b8bd4eb0027ce32eeaf595c21b0b6b4644b326e5b7bd80a1cf8db72e6c"

# =============================================================================
# EXTRACTION DATE RANGE (edit these to change extraction period)
# =============================================================================

# Define your date range here - both scripts will use these
EXTRACTION_START_DATE = "2025-01-27"
EXTRACTION_END_DATE = "2026-01-27"

# =============================================================================
# USER SEGMENTATION
# =============================================================================

# Users who bridge more than this are "whales"
WHALE_THRESHOLD_USD = 10000

# =============================================================================
# DATA PATHS
# =============================================================================

# Where raw data gets saved
RAW_DATA_DIR = "data/raw"
PROCESSED_DATA_DIR = "data/transformed"

# Output file names
BRIDGE_LOGS_FILE = f"{RAW_DATA_DIR}/etherscan_logs.csv"
LINEA_TXS_FILE = f"{RAW_DATA_DIR}/linea_transactions.csv"

# =============================================================================
# API SETTINGS
# =============================================================================

# Etherscan rate limits
REQUESTS_PER_SECOND = 4 # max 5 in API
REQUEST_DELAY = 0.15  # seconds between requests

# =============================================================================
# DATABASE SETTINGS
# =============================================================================

# Postgres Database connection settings
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "linea_analytics"
DB_USER = "postgres"
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")