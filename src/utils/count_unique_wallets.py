"""Quick script to count unique wallets in processed logs."""
import pandas as pd

#df = pd.read_csv(r"C:\Users\Longin\Desktop\Projects\metamask_linea_user_analytics\data\processed\processed_logs.csv")
df = pd.read_csv(r"C:\Users\Longin\Desktop\Projects\metamask_linea_user_analytics\temp\transformed_logs_full.csv")


unique_wallets = df["from_address"].nunique()
total_txs = len(df)


df["datetime"] = pd.to_datetime(df["datetime"])

# Get date range info
start_date = df["datetime"].min()
end_date = df["datetime"].max()
six_months_after_start = start_date + pd.Timedelta(days=360)
six_months_before_end = end_date - pd.Timedelta(days=360)

# First 6 months (from start)
first_6m_df = df[df["datetime"] <= six_months_after_start]
first_6m_wallets = first_6m_df["from_address"].nunique()
first_6m_txs = len(first_6m_df)

# Last 6 months (before end)
last_6m_df = df[df["datetime"] >= six_months_before_end]
last_6m_wallets = last_6m_df["from_address"].nunique()
last_6m_txs = len(last_6m_df)

print(f"\n--- Date Range ---")
print(f"Data starts: {start_date}")
print(f"Data ends: {end_date}")

print(f"\n--- Full Dataset ---")
print(f"Total transactions: {total_txs:,}")
print(f"Unique wallets: {unique_wallets:,}")

print(f"\n--- First 12 Months (until {six_months_after_start.date()}) ---")
print(f"Transactions: {first_6m_txs:,}")
print(f"Unique wallets: {first_6m_wallets:,}")

print(f"\n--- Last 12 Months (from {six_months_before_end.date()}) ---")
print(f"Transactions: {last_6m_txs:,}")
print(f"Unique wallets: {last_6m_wallets:,}")
