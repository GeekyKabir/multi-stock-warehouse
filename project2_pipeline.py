# ============================================================
# PROJECT 2: MULTI-STOCK DATA WAREHOUSE
# Author: Mohammad Kabir Sheikh
# Description: Automated ETL pipeline that extracts data for
#              5 companies, builds a star-schema data warehouse
#              in SQLite, and exports CSVs for Power BI.
# ============================================================

import requests
import json
import pandas as pd
import sqlite3
import os
import time   # We need this to wait between API calls


# ------------------------------------------------------------
# CONFIGURATION
# Add your API key. Free key: alphavantage.co/support/#api-key
# ------------------------------------------------------------

API_KEY    = "1VAODAOFACIR2ODO"
DB_FILE    = "warehouse.db"
OUTPUT_DIR = "powerbi_exports"
RAW_DIR    = "raw_data"

# The 5 companies we will pull data for
COMPANIES = {
    "TSLA":  "Tesla Inc",
    "AAPL":  "Apple Inc",
    "MSFT":  "Microsoft Corporation",
    "GOOGL": "Alphabet Inc",
    "AMZN":  "Amazon Inc"
}

# Sector info for our dimension table
SECTORS = {
    "TSLA":  "Consumer Discretionary",
    "AAPL":  "Technology",
    "MSFT":  "Technology",
    "GOOGL": "Communication Services",
    "AMZN":  "Consumer Discretionary"
}


# ============================================================
# EXTRACT
# ============================================================

def fetch_stock_data(symbol, api_key):
    print(f"  Fetching {symbol}...")

    url = "https://www.alphavantage.co/query"
    params = {
        "function":   "TIME_SERIES_DAILY",
        "symbol":     symbol,
        "outputsize": "compact",
        "apikey":     api_key
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"  ERROR fetching {symbol}: {e}")
        return None

    data = response.json()

    if "Error Message" in data:
        print(f"  API ERROR for {symbol}: {data['Error Message']}")
        return None

    if "Note" in data:
        print(f"  RATE LIMIT hit. Waiting 60 seconds...")
        time.sleep(60)
        return fetch_stock_data(symbol, api_key)  # Retry after waiting

    # Save raw JSON
    if not os.path.exists(RAW_DIR):
        os.makedirs(RAW_DIR)
    with open(f"{RAW_DIR}/{symbol}_raw.json", "w") as f:
        json.dump(data, f, indent=4)

    print(f"  {symbol} fetched and saved.")
    return data


# ============================================================
# TRANSFORM
# ============================================================

def transform_stock_data(data, symbol):
    time_series = data.get("Time Series (Daily)", {})
    if not time_series:
        return None

    # Parse into DataFrame
    df = pd.DataFrame.from_dict(time_series, orient="index")

    # Rename columns
    df = df.rename(columns={
        "1. open":   "open",
        "2. high":   "high",
        "3. low":    "low",
        "4. close":  "close",
        "5. volume": "volume"
    })

    # Fix data types
    for col in ["open", "high", "low", "close"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").astype("Int64")

    # Fix date
    df = df.reset_index().rename(columns={"index": "date"})
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    # Add symbol
    df["symbol"] = symbol

    # Enrich
    df["daily_change"]     = (df["close"] - df["open"]).round(2)
    df["daily_change_pct"] = ((df["daily_change"] / df["open"]) * 100).round(2)
    df["daily_range"]      = (df["high"] - df["low"]).round(2)

    return df


# ============================================================
# BUILD DIMENSION TABLES
# These are lookup tables that describe our data.
# Star schema = fact table + dimension tables.
# ============================================================

def build_dim_company(companies, sectors):
    print("\nBuilding dim_company table...")

    rows = []
    for i, (symbol, name) in enumerate(companies.items(), start=1):
        rows.append({
            "symbol_id":    i,
            "symbol":       symbol,
            "company_name": name,
            "sector":       sectors.get(symbol, "Unknown")
        })

    df = pd.DataFrame(rows)
    print(f"dim_company: {len(df)} companies")
    return df


def build_dim_date(all_dates):
    print("Building dim_date table...")

    # Get all unique dates across all stocks
    unique_dates = sorted(all_dates.unique())

    rows = []
    for i, date in enumerate(unique_dates, start=1):
        rows.append({
            "date_id":      i,
            "full_date":    date,
            "year":         date.year,
            "month":        date.month,
            "day":          date.day,
            "quarter":      (date.month - 1) // 3 + 1,
            "day_of_week":  date.strftime("%A"),       # Monday, Tuesday...
            "is_month_end": int(date == date + pd.offsets.MonthEnd(0))
        })

    df = pd.DataFrame(rows)
    print(f"dim_date: {len(df)} unique trading days")
    return df


# ============================================================
# BUILD FACT TABLE
# The central table — one row per stock per day.
# Uses foreign keys to link to dimension tables.
# ============================================================

def build_fact_table(all_data_df, dim_company, dim_date):
    print("Building fact_stock_prices table...")

    # Create lookup dictionaries for foreign keys
    # Instead of storing "TSLA" we store its ID number
    symbol_to_id = dict(zip(dim_company["symbol"], dim_company["symbol_id"]))
    date_to_id   = dict(zip(dim_date["full_date"], dim_date["date_id"]))

    fact = all_data_df.copy()

    # Replace symbol and date with their IDs (foreign keys)
    fact["symbol_id"] = fact["symbol"].map(symbol_to_id)
    fact["date_id"]   = fact["date"].map(date_to_id)

    # Keep only the columns we need in the fact table
    fact = fact[[
        "date_id",
        "symbol_id",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "daily_change",
        "daily_change_pct",
        "daily_range"
    ]]

    print(f"fact_stock_prices: {len(fact)} rows")
    return fact


# ============================================================
# LOAD INTO SQLITE WAREHOUSE
# ============================================================

def load_to_warehouse(dim_company, dim_date, fact_table, db_file):
    print(f"\nLoading into warehouse: {db_file}")

    conn = sqlite3.connect(db_file)

    dim_company.to_sql("dim_company",       conn, if_exists="replace", index=False)
    dim_date.to_sql("dim_date",             conn, if_exists="replace", index=False)
    fact_table.to_sql("fact_stock_prices",  conn, if_exists="replace", index=False)

    print("All 3 tables loaded into warehouse.")
    conn.close()


# ============================================================
# RUN ANALYTICAL QUERIES
# ============================================================

def run_warehouse_queries(db_file):
    print("\n" + "=" * 60)
    print("  WAREHOUSE QUERIES")
    print("=" * 60)

    conn = sqlite3.connect(db_file)

    # ----------------------------------------------------------
    # QUERY 1: Latest closing price for each company
    # ----------------------------------------------------------
    print("\nQUERY 1: Latest closing price per company")
    print("-" * 60)
    q1 = """
        SELECT
            c.symbol,
            c.company_name,
            d.full_date AS latest_date,
            f.close     AS latest_close
        FROM fact_stock_prices f
        JOIN dim_company c ON f.symbol_id = c.symbol_id
        JOIN dim_date    d ON f.date_id   = d.date_id
        WHERE d.full_date = (SELECT MAX(full_date) FROM dim_date)
        ORDER BY f.close DESC
    """
    print(pd.read_sql_query(q1, conn).to_string(index=False))

    # ----------------------------------------------------------
    # QUERY 2: Average monthly return per company
    # ----------------------------------------------------------
    print("\nQUERY 2: Average monthly return per company (%)")
    print("-" * 60)
    q2 = """
        SELECT
            c.symbol,
            d.year,
            d.month,
            ROUND(AVG(f.daily_change_pct), 3) AS avg_daily_return_pct,
            ROUND(AVG(f.close), 2)            AS avg_close
        FROM fact_stock_prices f
        JOIN dim_company c ON f.symbol_id = c.symbol_id
        JOIN dim_date    d ON f.date_id   = d.date_id
        GROUP BY c.symbol, d.year, d.month
        ORDER BY c.symbol, d.year, d.month
    """
    print(pd.read_sql_query(q2, conn).to_string(index=False))

    # ----------------------------------------------------------
    # QUERY 3: Most volatile company overall
    # ----------------------------------------------------------
    print("\nQUERY 3: Volatility ranking — most volatile company")
    print("-" * 60)
    q3 = """
        SELECT
            c.symbol,
            c.company_name,
            ROUND(AVG(f.daily_range), 2)          AS avg_daily_range,
            ROUND(AVG(ABS(f.daily_change_pct)), 2) AS avg_abs_change_pct,
            ROUND(MAX(f.daily_range), 2)           AS max_single_day_range
        FROM fact_stock_prices f
        JOIN dim_company c ON f.symbol_id = c.symbol_id
        GROUP BY c.symbol, c.company_name
        ORDER BY avg_abs_change_pct DESC
    """
    print(pd.read_sql_query(q3, conn).to_string(index=False))

    # ----------------------------------------------------------
    # QUERY 4: Best single day per company
    # ----------------------------------------------------------
    print("\nQUERY 4: Best single trading day per company")
    print("-" * 60)
    q4 = """
        SELECT
            c.symbol,
            d.full_date AS best_day,
            f.daily_change_pct AS pct_gain
        FROM fact_stock_prices f
        JOIN dim_company c ON f.symbol_id = c.symbol_id
        JOIN dim_date    d ON f.date_id   = d.date_id
        WHERE (c.symbol_id, f.daily_change_pct) IN (
            SELECT symbol_id, MAX(daily_change_pct)
            FROM fact_stock_prices
            GROUP BY symbol_id
        )
        ORDER BY pct_gain DESC
    """
    print(pd.read_sql_query(q4, conn).to_string(index=False))

    # ----------------------------------------------------------
    # QUERY 5: Sector performance comparison
    # ----------------------------------------------------------
    print("\nQUERY 5: Sector average performance")
    print("-" * 60)
    q5 = """
        SELECT
            c.sector,
            COUNT(DISTINCT c.symbol)              AS num_companies,
            ROUND(AVG(f.daily_change_pct), 3)     AS avg_daily_return_pct,
            ROUND(AVG(f.close), 2)                AS avg_close_price
        FROM fact_stock_prices f
        JOIN dim_company c ON f.symbol_id = c.symbol_id
        GROUP BY c.sector
        ORDER BY avg_daily_return_pct DESC
    """
    print(pd.read_sql_query(q5, conn).to_string(index=False))

    conn.close()
    print("\n" + "=" * 60)


# ============================================================
# EXPORT CSVs FOR POWER BI
# Power BI connects easily to CSV files.
# We export all 3 tables as separate CSVs.
# ============================================================

def export_for_powerbi(db_file, output_dir):
    print(f"\nExporting tables for Power BI to: {output_dir}/")

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    conn = sqlite3.connect(db_file)

    tables = ["dim_company", "dim_date", "fact_stock_prices"]

    for table in tables:
        df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
        filepath = f"{output_dir}/{table}.csv"
        df.to_csv(filepath, index=False)
        print(f"  Exported {table}.csv ({len(df)} rows)")

    # Also export a joined "flat" view — easiest for Power BI beginners
    flat_query = """
        SELECT
            d.full_date       AS date,
            c.symbol,
            c.company_name,
            c.sector,
            d.year,
            d.month,
            d.quarter,
            d.day_of_week,
            f.open,
            f.high,
            f.low,
            f.close,
            f.volume,
            f.daily_change,
            f.daily_change_pct,
            f.daily_range
        FROM fact_stock_prices f
        JOIN dim_company c ON f.symbol_id = c.symbol_id
        JOIN dim_date    d ON f.date_id   = d.date_id
        ORDER BY d.full_date, c.symbol
    """
    flat_df = pd.read_sql_query(flat_query, conn)
    flat_df.to_csv(f"{output_dir}/flat_view.csv", index=False)
    print(f"  Exported flat_view.csv ({len(flat_df)} rows) — use this in Power BI")

    conn.close()


# ============================================================
# MAIN: Run full pipeline
# ============================================================

if __name__ == "__main__":

    if API_KEY == "YOUR_API_KEY_HERE":
        print("Add your API key at the top of the file first.")
        exit()

    print("=" * 60)
    print("  PROJECT 2: MULTI-STOCK DATA WAREHOUSE PIPELINE")
    print("=" * 60)

    # --- EXTRACT + TRANSFORM all companies ---
    print("\nSTEP 1 + 2: Extract and Transform all companies")
    print("-" * 60)

    all_dataframes = []

    for i, symbol in enumerate(COMPANIES.keys()):
        raw_data = fetch_stock_data(symbol, API_KEY)

        if raw_data:
            df = transform_stock_data(raw_data, symbol)
            if df is not None:
                all_dataframes.append(df)

        # Wait 12 seconds between calls (free API = 5 calls/minute)
        # Skip waiting after the last company
        if i < len(COMPANIES) - 1:
            print(f"  Waiting 12 seconds before next API call...")
            time.sleep(12)

    if not all_dataframes:
        print("No data fetched. Check your API key and try again.")
        exit()

    # Combine all companies into one DataFrame
    all_data = pd.concat(all_dataframes, ignore_index=True)
    print(f"\nTotal rows across all companies: {len(all_data)}")

    # --- BUILD DIMENSION TABLES ---
    print("\nSTEP 3: Building warehouse dimension tables")
    print("-" * 60)
    dim_company = build_dim_company(COMPANIES, SECTORS)
    dim_date    = build_dim_date(all_data["date"])

    # --- BUILD FACT TABLE ---
    fact_table = build_fact_table(all_data, dim_company, dim_date)

    # --- LOAD INTO SQLITE ---
    print("\nSTEP 4: Loading into SQLite warehouse")
    print("-" * 60)
    load_to_warehouse(dim_company, dim_date, fact_table, DB_FILE)

    # --- RUN QUERIES ---
    run_warehouse_queries(DB_FILE)

    # --- EXPORT FOR POWER BI ---
    print("\nSTEP 5: Exporting CSVs for Power BI")
    print("-" * 60)
    export_for_powerbi(DB_FILE, OUTPUT_DIR)

    print("\n" + "=" * 60)
    print("  PIPELINE COMPLETE")
    print("=" * 60)
    print(f"\nWarehouse : {DB_FILE}")
    print(f"Power BI  : {OUTPUT_DIR}/flat_view.csv  <-- import this into Power BI")
    print(f"Raw data  : {RAW_DIR}/")
    print("\nNext: Open Power BI, import flat_view.csv, build your dashboard.")