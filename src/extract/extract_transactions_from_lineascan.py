"""
Lineascan Address Info Extractor
================================
Fetches all transactions for bridged wallets from Lineascan.
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
    LINEASCAN_API_KEY,
    LINEASCAN_URL,
    LINEA_CHAIN_ID,
    LINEA_START_BLOCK,
    LINEA_END_BLOCK,
    PROCESSED_DATA_DIR,
    RAW_DATA_DIR,
    REQUEST_DELAY
)


# =============================================================================
# SESSION (reuse connection)
# =============================================================================

session = requests.Session()


# =============================================================================
# FUNCTIONS
# =============================================================================

def load_unique_wallets(processed_logs_path, start_date="2025-07-31"):
    """Load unique wallet addresses from processed logs (filtered by date)."""
    df = pd.read_csv(processed_logs_path)
    
    # Filter to last 6 months only
    if "datetime" in df.columns:
        df["datetime"] = pd.to_datetime(df["datetime"])
        df = df[df["datetime"] >= start_date]
        print(f"📅 Filtered to transactions from {start_date} onwards")
        print(f"   Transactions in range: {len(df):,}")
    
    # The column name from processing - adjust if different
    if "from_address" in df.columns:
        wallets = df["from_address"].unique()
    elif "_from" in df.columns:
        wallets = df["_from"].unique()
    elif "from" in df.columns:
        wallets = df["from"].unique()
    else:
        # Try to find address column
        print(f"Available columns: {df.columns.tolist()}")
        raise ValueError("Could not find wallet address column")
    
    return list(wallets)


def get_transactions(address, start_block, end_block, max_retries=5):
    """Fetch all transactions for a wallet address on Linea."""
    all_txs = []
    page = 1
    
    while True:
        params = {
            "chainid": LINEA_CHAIN_ID,
            "module": "account",
            "action": "txlist",
            "address": address,
            "startblock": start_block,
            "endblock": end_block,
            "page": page,
            "offset": 1000,
            "sort": "asc",
            "apikey": LINEASCAN_API_KEY
        }
        
        # Retry logic for connection errors and empty responses
        data = None
        for attempt in range(max_retries):
            try:
                response = session.get(LINEASCAN_URL, params=params, timeout=30)
                
                # Check HTTP status
                if response.status_code != 200:
                    print(f"   ⚠️ HTTP {response.status_code} for {address[:10]}... (retry {attempt+1}/{max_retries})")
                    if attempt < max_retries - 1:
                        time.sleep((attempt + 1) * 2)
                        continue
                    return all_txs
                
                # Check for empty response
                if not response.text or response.text.strip() == "":
                    print(f"   ⚠️ Empty response for {address[:10]}... (retry {attempt+1}/{max_retries})")
                    if attempt < max_retries - 1:
                        time.sleep((attempt + 1) * 2)
                        continue
                    return all_txs
                
                data = response.json()
                break
                
            except requests.exceptions.JSONDecodeError as e:
                print(f"   ⚠️ JSON decode error for {address[:10]}... (retry {attempt+1}/{max_retries})")
                if attempt < max_retries - 1:
                    time.sleep((attempt + 1) * 2)
                    continue
                return all_txs
                
            except requests.exceptions.RequestException as e:
                print(f"   ⚠️ Request error: {type(e).__name__} (retry {attempt+1}/{max_retries})")
                if attempt < max_retries - 1:
                    time.sleep((attempt + 1) * 2)
                    continue
                return all_txs
                
            except Exception as e:
                print(f"   ⚠️ Unexpected error: {e} (retry {attempt+1}/{max_retries})")
                if attempt < max_retries - 1:
                    time.sleep((attempt + 1) * 2)
                    continue
                return all_txs
        
        if data is None:
            return all_txs
        
        # Check for API errors
        if data.get("status") != "1":
            result = data.get("result", "")
            if "No transactions found" in str(result):
                break
            break
        
        batch = data.get("result", [])
        if not batch:
            break
        
        # Add wallet address to each tx for reference
        for tx in batch:
            tx["wallet"] = address
        
        all_txs.extend(batch)
        
        if len(batch) < 1000:
            break

        
        page += 1
        time.sleep(REQUEST_DELAY)
    
    return all_txs


def extract_all_wallet_transactions(wallets, output_path, start_block, end_block, checkpoint_every=500, max_workers=4):
    """Extract transactions for all wallets with PARALLEL processing."""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    
    output_path = Path(PROJECT_ROOT) / output_path
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    all_transactions = []
    total = len(wallets)
    processed = 0
    wallets_with_txs = 0
    
    print(f"📥 Extracting Linea transactions for {total:,} wallets")
    print(f"   Block range: {start_block:,} → {end_block:,}")
    print(f"   Using {max_workers} parallel workers")
    print(f"   Output: {output_path}")
    print("=" * 60)
    
    # Process in parallel batches
    def fetch_wallet(wallet):
        return wallet, get_transactions(wallet, start_block, end_block)
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_wallet, w): w for w in wallets}
        
        for future in as_completed(futures):
            wallet, txs = future.result()
            processed += 1
            
            if txs:
                all_transactions.extend(txs)
                wallets_with_txs += 1
            
            # Progress update every 100 wallets
            if processed % 100 == 0 or processed == total:
                print(f"   [{processed:,}/{total:,}] Wallets processed | {len(all_transactions):,} txs | {wallets_with_txs:,} active")
            
            # Checkpoint save
            if processed % checkpoint_every == 0 and all_transactions:
                df = pd.DataFrame(all_transactions)
                df.to_csv(output_path, index=False)
                print(f"   💾 Checkpoint saved: {len(df):,} rows")
    
    # Final save
    if all_transactions:
        df = pd.DataFrame(all_transactions)
        df.to_csv(output_path, index=False)
        print(f"\n✅ Done! Saved {len(df):,} transactions from {wallets_with_txs:,} active wallets")
        return df
    else:
        print("\n⚠️ No transactions found for any wallet")
        return pd.DataFrame()


# =============================================================================
# MAIN - Run extraction
# =============================================================================

if __name__ == "__main__":
    
    if not LINEASCAN_API_KEY:
        print("❌ No LINEASCAN_API_KEY in .env file!")
        exit(1)
    
    print("🚀 Linea Wallet Transaction Extraction")
    print("=" * 60)
    print(f"📅 Date range: Last 6 months (blocks {LINEA_START_BLOCK:,} → {LINEA_END_BLOCK:,})")
    
    # Load unique wallets from processed logs
    processed_logs = Path(PROJECT_ROOT) / PROCESSED_DATA_DIR / "processed_logs.csv"
    
    if not processed_logs.exists():
        print(f"❌ Processed logs not found: {processed_logs}")
        exit(1)
    
    wallets = load_unique_wallets(processed_logs)
    print(f"📋 Found {len(wallets):,} unique wallets to process")
    
    # Extract all transactions
    output_file = f"{RAW_DATA_DIR}/linea_transactions.csv"
    df = extract_all_wallet_transactions(
        wallets, 
        output_file,
        start_block=LINEA_START_BLOCK,
        end_block=LINEA_END_BLOCK
    )
    
    if not df.empty:
        print(f"\n📄 Sample row:")
        print(df.iloc[0].to_dict())
