"""
Etherscan API Client
====================
Fetches blockchain data from Etherscan and saves to CSV.
"""

import sys
import time
import requests
import pandas as pd
from pathlib import Path

# Add project root to path so we can import config
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from utils.config import (
    ETHERSCAN_API_KEY,
    ETHERSCAN_URL,
    ETHEREUM_CHAIN_ID,
    LINEA_BRIDGE_CONTRACT,
    MESSAGE_SENT_TOPIC,
    EXTRACTION_START_DATE,
    EXTRACTION_END_DATE,
    BRIDGE_LOGS_FILE,
    REQUEST_DELAY
)
from utils.block_utils import get_eth_block_by_date


# =============================================================================
# SESSION (reuse connection)
# =============================================================================

session = requests.Session()


# =============================================================================
# FUNCTIONS
# =============================================================================

def get_logs(address, topic0, from_block, to_block):
    """Fetch event logs from a smart contract (single page, max 1000)."""
    params = {
        "chainid": ETHEREUM_CHAIN_ID,
        "module": "logs",
        "action": "getLogs",
        "address": address,
        "topic0": topic0,
        "fromBlock": from_block,
        "toBlock": to_block,
        "page": 1,
        "offset": 1000,
        "apikey": ETHERSCAN_API_KEY
    }
    
    response = session.get(ETHERSCAN_URL, params=params, timeout=30)
    data = response.json()
    
    if data.get("status") == "1":
        return data.get("result", [])
    
    if "No records found" in str(data.get("result", "")):
        return []
    
    print(f"❌ Error: {data.get('message')} - {data.get('result')}")
    return []


def get_logs_for_range(address, topic0, from_block, to_block, max_retries=3):
    """Fetch logs for a specific block range with pagination (max 10,000 per range)."""
    logs = []
    page = 1
    
    while True:
        params = {
            "chainid": ETHEREUM_CHAIN_ID,
            "module": "logs",
            "action": "getLogs",
            "address": address,
            "topic0": topic0,
            "fromBlock": from_block,
            "toBlock": to_block,
            "page": page,
            "offset": 1000,
            "apikey": ETHERSCAN_API_KEY
        }
        
        # Retry logic for connection errors
        for attempt in range(max_retries):
            try:
                response = session.get(ETHERSCAN_URL, params=params, timeout=30)
                data = response.json()
                break
            except Exception as e:
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 2
                    print(f"   ⏳ Retry {attempt + 1}/{max_retries} after {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    print(f"   ❌ Failed after {max_retries} retries: {e}")
                    return logs
        
        # Check for API errors
        if data.get("status") != "1":
            result = data.get("result", "")
            if "No records found" in str(result):
                break  # Normal end - no more records
            if result:  # Only print if there's an actual error message
                print(f"   ⚠️ API issue: {result}")
            break
        
        batch = data.get("result", [])
        if not batch:
            break
        
        logs.extend(batch)
        
        if len(batch) < 1000:
            break
        
        page += 1
        time.sleep(REQUEST_DELAY)
    
    return logs


def get_all_logs(address, topic0, from_block, to_block, chunk_size=100000):
    """Fetch ALL logs by splitting into block range chunks to bypass 10k limit."""
    all_logs = []
    current_block = from_block
    chunk_num = 1
    
    print(f"📥 Fetching logs from {address[:10]}...")
    print(f"   Blocks: {from_block} → {to_block}")
    print(f"   Using chunk size: {chunk_size:,} blocks")
    
    while current_block <= to_block:
        chunk_end = min(current_block + chunk_size - 1, to_block)
        
        print(f"\n📦 Chunk {chunk_num}: blocks {current_block:} → {chunk_end:}")
        chunk_logs = get_logs_for_range(address, topic0, current_block, chunk_end)
        all_logs.extend(chunk_logs)
        
        print(f"   Got {len(chunk_logs):,} logs (total: {len(all_logs):,})")
        
        current_block = chunk_end + 1
        chunk_num += 1
        time.sleep(REQUEST_DELAY)
    
    print(f"\n✅ Done! Got {len(all_logs):,} total logs")
    return all_logs


def save_to_csv(logs, output_path):
    """Save raw logs to CSV file."""
    output_path = Path(PROJECT_ROOT) / output_path
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    df = pd.DataFrame(logs)
    df.to_csv(output_path, index=False)
    
    print(f"💾 Saved {len(df)} rows to {output_path}")
    return df


# =============================================================================
# MAIN - Run extraction
# =============================================================================

if __name__ == "__main__":
    
    if not ETHERSCAN_API_KEY:
        print("❌ No ETHERSCAN_API_KEY in .env file!")
        exit(1)
    
    print("🚀 Linea Bridge Log Extraction")
    print("=" * 50)
    
    # Fetch block numbers for the date range
    print(f"📅 Date range: {EXTRACTION_START_DATE} → {EXTRACTION_END_DATE}")
    print("   Looking up block numbers...")
    start_block = get_eth_block_by_date(EXTRACTION_START_DATE, ETHERSCAN_API_KEY)
    end_block = get_eth_block_by_date(EXTRACTION_END_DATE, ETHERSCAN_API_KEY)
    print(f"   Blocks: {start_block:,} → {end_block:,}")
    
    # Fetch all bridge logs
    logs = get_all_logs(
        address=LINEA_BRIDGE_CONTRACT,
        topic0=MESSAGE_SENT_TOPIC,
        from_block=start_block,
        to_block=end_block
    )
    
    if logs:
        df = save_to_csv(logs, BRIDGE_LOGS_FILE)
        print(f"\n📄 Sample row:")
        print(df.iloc[0].to_dict())
