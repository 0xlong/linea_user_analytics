# MetaMask-to-Linea Growth & Retention Engine 🚀

Welcome to the **MetaMask-to-Linea Retention Engine**! This project is all about answering one big question: **"Are users who bridge to Linea sticking around, or are they just passing through?"**

We track users bridging from Ethereum Mainnet to Linea and analyze their ongoing activity to build cool cohort retention matrices. It's built for the MetaMask Product Team (and as a portfolio piece!) to spot trends, identify whales 🐋 vs. retail 🐟, and measure real engagement.

---

## 🏗️ Architecture & Tech Stack

We use a modern data stack to keep things extracting, loading, and transforming smoothly:

*   **Extraction**: Python scripts hitting Etherscan & Lineascan APIs.
*   **Load (Storage)**: PostgreSQL.
*   **Transformation**: dbt (data build tool) for modeling and business logic.

### Data Flow
1.  **Extract**: Pull `MessageSent` logs from the Linea Canonical Bridge on Ethereum.
2.  **Extract**: Detailed transaction history for those wallets on Linea.
3.  **Load**: Push raw CSVs into PostgreSQL.
4.  **Transform**: Use dbt to clean data, build user segments, and calculate retention.

---

## 🛠️ Setup & How to Run

### Prerequisities
*   Python 3.10+
*   PostgreSQL database
*   API Keys for Etherscan and Lineascan

### 1. Environment Setup
Create a `.env` file (copy from `.env.example`) and add your secrets:
```bash
ETHERSCAN_API_KEY=your_key
LINEASCAN_API_KEY=your_key
POSTGRES_DB=linea_analytics
# ... other DB credentials
```

### 2. Run Extraction
Grab the data from the blockchain. (Note: The extraction filters for the last ~6 months to respect API rate limits).
```bash
python src/extraction/extract_bridge_logs.py
python src/extraction/extract_linea_activity.py
```

### 3. Load to Database
Simulate a loading process (or if scripts support direct DB load):
```bash
# Example command if you have a loader script, otherwise import CSVs to Postgres manually/via script
python src/loading/load_to_postgres.py 
```
*Note: Ensure your Postgres password is set in your environment variables, e.g., `$env:POSTGRES_PASSWORD="your_password"` on Windows.*

### 4. Run dbt Models
Transform that raw data into insights!
```bash
cd dbt_project
dbt deps
dbt build
```

---

## 📊 Key Metrics

*   **Cohort Retention Rate**: Percentage of users coming back Month 1, Month 2, etc.
*   **Whale vs Retail**: Segmenting users by bridge volume (e.g., >$10k = Whale).
*   **Churn Risk**: Identifying users inactive for 2+ months.

---

## 📁 Project Structure

*   `src/`: Python extraction & processing scripts.
*   `dbt_project/`: The SQL magic. Models, seeds, and tests.
*   `data/`: Raw and processed CSVs (ignored by git).
*   `project_context/`: Detailed docs & architecture decisions.

---
