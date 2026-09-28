import os
import sys
import argparse
import glob
import pandas as pd
import numpy as np
from datetime import datetime

DATA_DIR = "historic_data"
INITIAL_INVESTMENT_PER_STOCK = 100000.0

def load_dynamic_universe(universe_file_path):
    if not os.path.exists(universe_file_path):
        sys.exit(f"❌ Missing universe file: {universe_file_path}")
    try:
        df = pd.read_csv(universe_file_path)
        df.columns = df.columns.str.strip().str.title()
        target_series = df['Symbol'] if 'Symbol' in df.columns else df.iloc[:, 0]
        return sorted(list(set([str(s).strip().upper() for s in target_series.dropna()])))
    except Exception as e:
        sys.exit(f"💥 Universe parse error: {e}")

def load_and_merge_data(symbols):
    master_list = []
    for symbol in symbols:
        fpath = os.path.join(DATA_DIR, f"{'LTM' if symbol == 'LTIM' else symbol}.csv")
        if os.path.exists(fpath):
            df = pd.read_csv(fpath, parse_dates=['Date'], index_col='Date')
            if not df.empty and 'Close' in df.columns:
                master_list.append(df['Close'].rename(symbol))
    if not master_list:
        sys.exit("❌ Error: No matching historical CSV files found in directory.")
    return pd.concat(master_list, axis=1).sort_index().ffill().bfill()

def get_rebalance_dates(start_date, end_date, rebalance_period_months):
    dates = []
    curr = pd.Timestamp(datetime(start_date.year, start_date.month, 1))
    while curr <= end_date:
        dates.append(curr)
        curr = curr + pd.DateOffset(months=rebalance_period_months)
    return sorted(list(set(dates)))

def calculate_momentum(price_df, current_date, top_gainer_for_period):
    lookback_date = current_date - pd.DateOffset(months=top_gainer_for_period)
    try:
        p_now = price_df.loc[:current_date].iloc[-1]
        p_then = price_df.loc[:lookback_date].iloc[-1]
        return ((p_now - p_then) / p_then).dropna()
    except IndexError:
        return pd.Series(dtype=float)

def run_momentum_strategy(start_str, lookback, top_n, uni_file, freq):
    symbols = load_dynamic_universe(uni_file)
    prices = load_and_merge_data(symbols)
    
    start_date = pd.Timestamp(start_str)
    end_date = prices.index.max()
    
    min_start = prices.index.min() + pd.DateOffset(months=lookback)
    if start_date < min_start:
        print(f"⚠️ Start date adjusted to match historical lookback limits: {min_start.strftime('%Y-%m-%d')}")
        start_date = min_start

    rebalance_schedule = get_rebalance_dates(start_date, end_date, freq)
    if not rebalance_schedule:
        print("❌ No valid rebalance dates found within this timeframe window.")
        return

    portfolio = {}
    initialized = False
    
    print(f"\n🚀 Executing Strategy Backtester...")
    print(f"📌 [CONFIG] Start: {start_date.strftime('%Y-%m-%d')} | Period: {lookback} Mon | Portfolio: {top_n} Stocks | Rebalance Freq: {freq} Month(s)")
    
    for r_date in rebalance_schedule:
        actual_idx = prices.index.get_indexer([r_date], method='nearest')
        
        # FIXED: Extracting the actual raw scalar element using [0] to stop DatetimeIndex array crashes
        actual_date = prices.index[actual_idx[0]]
        print(f"\n📅 --- Rebalance Date: {actual_date.strftime('%Y-%m-%d')} ---")
        
        momentum_scores = calculate_momentum(prices, actual_date, lookback)
        if momentum_scores.empty:
            print("⚠️ Insufficient lookback history, skipping checkpoint.")
            continue
            
        top_gainers = momentum_scores.nlargest(top_n).index.tolist()
        
        if not initialized:
            print(f"🆕 Initial Allocation into Top {top_n} Leaders:")
            for sym in top_gainers:
                entry_p = float(prices.loc[actual_date, sym])
                portfolio[sym] = {
                    'capital': INITIAL_INVESTMENT_PER_STOCK, 
                    'entry_price': entry_p,
                    'initial_capital': INITIAL_INVESTMENT_PER_STOCK,
                    'initial_price': entry_p
                }
                print(f"  🔹 Invested ₹{INITIAL_INVESTMENT_PER_STOCK:,.2f} into {sym} at Price: ₹{entry_p:,.2f}")
            initialized = True
        else:
            exiting_capital_pool = 0.0
            retained_stocks = []
            portfolio_symbols = list(portfolio.keys())
            
            for sym in portfolio_symbols:
                current_p = float(prices.loc[actual_date, sym])
                entry_p = portfolio[sym]['entry_price']
                initial_cap = portfolio[sym]['capital']
                
                current_val = initial_cap * (current_p / entry_p)
                true_pnl = current_val - portfolio[sym]['initial_capital']
                
                if sym in top_gainers:
                    portfolio[sym]['capital'] = current_val
                    portfolio[sym]['entry_price'] = current_p
                    retained_stocks.append(sym)
                    print(f"  ✅ RETAIN: {sym:<10} | Current Value: ₹{current_val:,.2f} (Total PnL from Entry: ₹{true_pnl:+,.2f})")
                else:
                    exiting_capital_pool += current_val
                    print(f"  ❌ EXIT:   {sym:<10} | Sold position at: ₹{current_val:,.2f} (Total PnL from Entry: ₹{true_pnl:+,.2f})")
                    del portfolio[sym]
            
            new_additions = [s for s in top_gainers if s not in retained_stocks]
            if new_additions and exiting_capital_pool > 0:
                capital_per_new_stock = exiting_capital_pool / len(new_additions)
                print(f"  📥 Reinvesting ₹{exiting_capital_pool:,.2f} balance pool into {len(new_additions)} new entrants:")
                for sym in new_additions:
                    entry_p = float(prices.loc[actual_date, sym])
                    portfolio[sym] = {
                        'capital': capital_per_new_stock, 
                        'entry_price': entry_p,
                        'initial_capital': capital_per_new_stock,
                        'initial_price': entry_p
                    }
                    print(f"    ➕ ENTER: {sym:<10} at Price: ₹{entry_p:,.2f}")
                    
        total_value = sum([pos['capital'] for pos in portfolio.values()])
        print(f"💰 Total Portfolio Net Asset Value at checkpoint: ₹{total_value:,.2f}")

    print("\n" + "="*60 + "\n🏁 FINAL PORTFOLIO STATUS AS OF TODAY (" + end_date.strftime('%Y-%m-%d') + ")\n" + "="*60)
    final_total_value = 0.0
    holding_summary = []
    
    for sym, details in portfolio.items():
        entry_p = details['entry_price']
        initial_cap = details['capital']
        today_p = float(prices.loc[end_date, sym])
        
        current_val = initial_cap * (today_p / entry_p)
        pnl_pct = ((today_p - details['initial_price']) / details['initial_price']) * 100
        final_total_value += current_val
        
        holding_summary.append({
            "Stock": sym,
            "Current Value Today": f"₹{current_val:,.2f}",
            "Total Return Since Entry": f"{pnl_pct:+.2f}%"
        })
        
    if holding_summary:
        print(pd.DataFrame(holding_summary).to_string(index=False))
    
    initial_total_capital = top_n * INITIAL_INVESTMENT_PER_STOCK
    overall_pnl = final_total_value - initial_total_capital
    overall_return_pct = (overall_pnl / initial_total_capital) * 100
    
    print("-"*60)
    print(f"💵 Initial Total Principal Invested: ₹{initial_total_capital:,.2f}")
    print(f"🚀 Final Portfolio Value Today:     ₹{final_total_value:,.2f}")
    print(f"📊 Net Absolute Return:            ₹{overall_pnl:+,.2f} ({overall_return_pct:+.2f}%)")
    print("="*60)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--universe_file", type=str, default="nifty_list.csv")
    parser.add_argument("--start_date", type=str, default="2024-01-01")
    parser.add_argument("--top_gainer_for_period", type=int, default=12)
    parser.add_argument("--top_n", type=int, default=7)
    parser.add_argument("--rebalance_period_months", type=int, default=3, choices=range(1, 13))
    args = parser.parse_args()
    
    run_momentum_strategy(args.start_date, args.top_gainer_for_period, args.top_n, args.universe_file, args.rebalance_period_months)
