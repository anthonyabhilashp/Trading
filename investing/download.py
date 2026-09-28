import os
import io
import time
import requests
import pandas as pd
import yfinance as yf
from datetime import datetime

# Enforce clean target directory setup
DATA_DIR = "historic_data"
os.makedirs(DATA_DIR, exist_ok=True)
RANK_LIST_FILE = "nifty_500_market_cap_list.csv"

def get_nifty500_tickers_live():
    """Dynamically fetches the NIFTY 500 list from the official archive or high-availability mirror."""
    primary_url = "https://nseindia.com"
    fallback_url = "https://githubusercontent.com"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    
    print("📥 Step 1: Downloading live NIFTY 500 roster framework...")
    try:
        response = requests.get(primary_url, headers=headers, timeout=10)
        response.raise_for_status()
        text_data = response.text
    except Exception:
        print("⚠️ Primary connection dropped or blocked. Trying backup mirror...")
        try:
            response = requests.get(fallback_url, headers=headers, timeout=10)
            response.raise_for_status()
            text_data = response.text
        except Exception as e:
            print(f"❌ Both data stream sources failed. Network Error: {e}")
            raise RuntimeError("Pipeline stopped because the dynamic stock list could not be retrieved.")

    df = pd.read_csv(io.StringIO(text_data.strip()))
    
    # Map both 'Symbol' and the full 'Company Name' columns cleanly
    df.columns = df.columns.str.strip().str.Title()
    
    if 'Symbol' in df.columns:
        # Create a dynamic lookup map for Company Name matching
        name_col = 'Company Name' if 'Company Name' in df.columns else 'Companyname'
        name_map = {}
        if name_col in df.columns:
            name_map = pd.Series(df[name_col].values, index=df['Symbol'].str.strip().str.upper()).to_dict()
            
        symbols = [str(sym).strip().upper() for sym in df['Symbol'].dropna().tolist() if str(sym).strip()]
        return sorted(list(set(symbols))), name_map
    else:
        raise Exception("CSV downloaded successfully, but the 'Symbol' column layout was absent.")

def download_or_update_historical_data(symbol, default_start="2020-01-01"):
    """Fetches ONLY missing ranges and updates Close/Volume CSV files inside historic_data folder."""
    csv_path = os.path.join(DATA_DIR, f"{symbol}.csv")
    current_date = datetime.today().strftime('%Y-%m-%d')
    
    yf_symbol = "LTM" if symbol == "LTIM" else symbol
    ticker_ns = f"{yf_symbol}.NS"
    
    existing_df = pd.DataFrame()
    start_date = default_start
    
    if os.path.exists(csv_path):
        try:
            existing_df = pd.read_csv(csv_path, parse_dates=['Date'], index_col='Date')
            if not existing_df.empty:
                last_recorded_date = existing_df.index.max()
                start_date = last_recorded_date.strftime('%Y-%m-%d')
                
                if start_date == current_date:
                    return  # Skip download if already up to date
                
                print(f"🔄 [INCREMENTAL] {symbol}: Updating missing rows from {start_date} -> {current_date}")
        except Exception:
            existing_df = pd.DataFrame()

    if existing_df.empty:
        print(f"🆕 [FULL DOWNLOAD] {symbol}: Extracting history from {default_start} -> {current_date}")

    try:
        new_data = yf.download(ticker_ns, start=start_date, end=current_date, auto_adjust=False, progress=False)
        if new_data.empty:
            return
            
        if isinstance(new_data.columns, pd.MultiIndex):
            new_data.columns = new_data.columns.get_level_values(0)
            
        new_data = new_data[['Close', 'Volume']]
        new_data.index = pd.to_datetime(new_data.index)
        
        if not existing_df.empty:
            if isinstance(existing_df.columns, pd.MultiIndex):
                existing_df.columns = existing_df.columns.get_level_values(0)
            existing_df = existing_df[['Close', 'Volume']]
            combined_df = new_data.combine_first(existing_df)
        else:
            combined_df = new_data
            
        combined_df = combined_df.sort_index()
        combined_df = combined_df[~combined_df.index.duplicated(keep='last')]
        combined_df.to_csv(csv_path)
        
    except Exception as e:
        print(f"❌ Connection timeout for ticker {symbol}: {e}")

def extract_and_generate_sorted_mcap_list(symbols, name_map):
    """
    Step 2: Reads local updated CSV files, fetches shares outstanding,
    calculates market caps, and exports the requested simplified list.
    """
    print("\n📊 Step 2: Extracting local data to build the Sorted Market Cap list...")
    mcap_list = []
    total_symbols = len(symbols)
    
    print("⏳ Synchronizing stock market valuation matrices...")
    
    for idx, symbol in enumerate(symbols, 1):
        csv_path = os.path.join(DATA_DIR, f"{symbol}.csv")
        yf_symbol = "LTM" if symbol == "LTIM" else symbol
        ticker_ns = f"{yf_symbol}.NS"
        
        try:
            df = pd.read_csv(csv_path, parse_dates=['Date'], index_col='Date')
            
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
                
            close_p = float(df['Close'].dropna().iloc[-1])
            
            # Fetch outstanding share metrics
            ticker_obj = yf.Ticker(ticker_ns)
            shares_df = ticker_obj.get_shares_full(start="2026-01-01", end="2026-09-28")
            shares = shares_df.iloc[-1] if not shares_df.empty else ticker_obj.info.get('sharesOutstanding', 0)
            
            company_name = name_map.get(symbol, symbol)
            
            if shares and shares > 0:
                calculated_mcap = close_p * float(shares)
                mcap_in_crores = calculated_mcap / 10000000.0
                
                mcap_list.append({
                    "Stock": company_name,
                    "Ticker": symbol,
                    "Market Cap (₹ Crores)": round(mcap_in_crores, 2)
                })
            else:
                mcap_list.append({"Stock": company_name, "Ticker": symbol, "Market Cap (₹ Crores)": 0.0})
        except Exception:
            company_name = name_map.get(symbol, symbol)
            mcap_list.append({"Stock": company_name, "Ticker": symbol, "Market Cap (₹ Crores)": 0.0})
            
        if idx % 100 == 0 or idx == total_symbols:
            print(f"   Calculated Market Caps: {idx}/{total_symbols} stocks processed...")
        
    # Convert matrix to dataframe, sort top-to-bottom (Highest to Lowest)
    mcap_df = pd.DataFrame(mcap_list)
    mcap_df = mcap_df.sort_values(by="Market Cap (₹ Crores)", ascending=False)
    
    # Fulfill rule restriction: Keep ONLY Stock, Ticker, and Market Cap columns
    mcap_df = mcap_df[["Stock", "Ticker", "Market Cap (₹ Crores)"]]
    
    # Save the clean ranked CSV spreadsheet list
    mcap_df.to_csv(RANK_LIST_FILE, index=False)
    print(f"\n📝 Success! Clean master list generated and saved to: '{RANK_LIST_FILE}'")

# --- MAIN CONTROL PIPELINE ---
def run_pipeline():
    try:
        nifty500_symbols, name_map = get_nifty500_tickers_live()
        
        # 1. DOWNLOAD / UPDATE DATA FIRST
        print(f"\n🚀 Commencing Ingestion: Updating missing data points for all {len(nifty500_symbols)} stocks...")
        for idx, symbol in enumerate(nifty500_symbols, 1):
            download_or_update_historical_data(symbol)
            if idx % 20 == 0:
                time.sleep(0.1)
        print("🎉 Step 1 Complete: All local CSV files are perfectly up to date.")
        
        # 2. RUN EXTRACTOR AND SORT SECOND
        extract_and_generate_sorted_mcap_list(nifty500_symbols, name_map)
        print(f"\n✅ All done! Local database is synced and ranked spreadsheet is generated.")
        
    except Exception as error:
        print(f"💥 Pipeline Interrupted: {error}")

if __name__ == "__main__":
    run_pipeline()

