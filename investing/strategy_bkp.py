import os
import sys
import argparse
import glob
import pandas as pd
import numpy as np
from datetime import datetime

DATA_DIR = "historic_data"
UNIVERSE_FILE = "nifty_midcap_150.csv"
INITIAL_INVESTMENT_PER_STOCK = 100000.0

def load_midcap_150_universe():
    print(f"🔎 Parsing universe mapping from '{UNIVERSE_FILE}'...")
    if not os.path.exists(UNIVERSE_FILE):
        raise FileNotFoundError(f"❌ '{UNIVERSE_FILE}' is missing!")
    try:
        df = pd.read_csv(UNIVERSE_FILE)
        df.columns = df.columns.str.strip().str.title()
        if 'Symbol' in df.columns:
            return sorted(list(set([str(s).strip().upper() for s in df['Symbol'].dropna()])))
        else:
            raise Exception("The 'Symbol' column marker is missing in CSV.")
    except Exception as e:
        print(f"💥 Failed to parse tracking universe configuration file: {e}")
        raise e

def load_and_merge_data(midcap_symbols):
    print("📋 Compiling historical data matrix from offline blocks...")
    master_list = []
    for symbol in midcap_symbols:
        lookup_symbol = "LTM" if symbol == "LTIM" else symbol
        fpath = os.path.join(DATA_DIR, f"{lookup_symbol}.csv")
        if os.path.exists(fpath):
            df = pd.read_csv(fpath, parse_dates=['Date'], index_col='Date')
            if not df.empty and 'Close' in df.columns:
                master_list.append(df['Close'].rename(symbol))
    if not master_list:
        raise FileNotFoundError(f"❌ No matching data found in './{DATA_DIR}'.")
    master_df = pd.concat(master_list, axis=1).sort_index()
    return master_df.ffill().bfill()

def get_rebalance_dates(start_date, end_date):
    dates = []
    current_year = start_date.year
    target_months = [int(m) for m in "1,4,7,10".split(",")]
    while current_year <= end_date.year:
        for month in target_months:
            r_date = pd.Timestamp(datetime(current_year, month, 1))
            if start_date <= r_date <= end_date:
                dates.append(r_date)
        current_year += 1
    return sorted(list(set(dates)))

def calculate_momentum(price_df, current_date, top_gainer_for_period):
    lookback_date = current_date - pd.DateOffset(months=top_gainer_for_period)
    try:
        price_now = price_df.loc[:current_date].iloc[-1]
        price_then = price_df.loc[:lookback_date].iloc[-1]
        return ((price_now - price_then) / price_then).dropna()
    except IndexError:
        return pd.Series(dtype=float)

def run_momentum_strategy(investment_start_str, top_gainer_for_period, top_n):
    midcap_symbols = load_midcap_150_universe()
    prices = load_and_merge_data(midcap_symbols)
    strategy_start = pd.Timestamp(investment_start_str)
    strategy_end = prices.index.max()
    min_start = prices.index.min() + pd.DateOffset(months=top_gainer_for_period)
    if strategy_start < min_start:
        print(f"⚠️ Start date adjusted to match limits: {min_start.strftime('%Y-%m-%d')}")
        strategy_start = min_start
    rebalance_schedule = get_rebalance_dates(strategy_start, strategy_end)
    if not rebalance_schedule:
        print("❌ No valid quarter start rebalance dates found within the window.")
        return
    portfolio = {}
    initialized = False
    print(f"\n🚀 Simulating NIFTY MIDCAP 150 Performance Strategy...")
    print(f"📌 [CONFIG] Start: {strategy_start.strftime('%Y-%m-%d')} | Period: {top_gainer_for_period} Mon | Portfolio: {top_n} Stocks")
    for r_date in rebalance_schedule:
        actual_date_idx = prices.index.get_indexer([r_date], method='nearest')
        # FIX: Extract the actual scalar Timestamp instead of a DatetimeIndex slice container
        actual_date = prices.index[actual_date_idx[0]]
        print(f"\n📅 --- Rebalance Date: {actual_date.strftime('%Y-%m-%d')} ---")
        momentum_scores = calculate_momentum(prices, actual_date, top_gainer_for_period)
        if momentum_scores.empty:
            print("⚠️ Insufficient historical data, skipping checkpoint.")
            continue
        top_gainers = momentum_scores.nlargest(top_n).index.tolist()
        if not initialized:
            print(f"🆕 Initial Allocation into Top {top_n} Leaders:")
            for sym in top_gainers:
                entry_p = float(prices.loc[actual_date, sym])
                portfolio[sym] = {'capital': INITIAL_INVESTMENT_PER_STOCK, 'entry_price': entry_p}
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
                pnl = current_val - initial_cap
                if sym in top_gainers:
                    portfolio[sym]['capital'] = current_val
                    portfolio[sym]['entry_price'] = current_p
                    retained_stocks.append(sym)
                    print(f"  ✅ RETAIN: {sym} | Value: ₹{current_val:,.2f} (PnL: ₹{pnl:+,.2f})")
                else:
                    exiting_capital_pool += current_val
                    del portfolio[sym]
                    print(f"  ❌ EXIT: {sym} | Sold at: ₹{current_val:,.2f} (PnL: ₹{pnl:+,.2f})")
            new_additions = [s for s in top_gainers if s not in retained_stocks]
            if new_additions and exiting_capital_pool > 0:
                capital_per_new_stock = exiting_capital_pool / len(new_additions)
                print(f"  📥 Reinvesting ₹{exiting_capital_pool:,.2f} into new entrants:")
                for sym in new_additions:
                    entry_p = float(prices.loc[actual_date, sym])
                    portfolio[sym] = {'capital': capital_per_new_stock, 'entry_price': entry_p}
                    print(f"    ➕ ENTER: {sym} at Price: ₹{entry_p:,.2f}")
        total_value = sum([pos['capital'] for pos in portfolio.values()])
        print(f"💰 Portfolio Value at checkpoint: ₹{total_value:,.2f}")

    print("\n" + "="*60)
    print(f"🏁 FINAL PORTFOLIO STATUS AS OF TODAY ({strategy_end.strftime('%Y-%m-%d')})")
    print("="*60)
    final_total_value = 0.0
    holding_summary = []
    for sym, details in portfolio.items():
        entry_p = details['entry_price']
        initial_cap = details['capital']
        today_p = float(prices.loc[strategy_end, sym])
        current_val = initial_cap * (today_p / entry_p)
        pnl_pct = ((today_p - entry_p) / entry_p) * 100
        final_total_value += current_val
        holding_summary.append({
            "Stock": sym,
            "Allocated Capital": f"₹{initial_cap:,.2f}",
            "Current Value Today": f"₹{current_val:,.2f}",
            "Return since Rebalance": f"{pnl_pct:+.2f}%"
        })
    if holding_summary:
        print(pd.DataFrame(holding_summary).to_string(index=False))
    initial_total_capital = top_n * INITIAL_INVESTMENT_PER_STOCK
    overall_pnl = final_total_value - initial_total_capital
    overall_return_pct = (overall_pnl / initial_total_capital) * 100
    print("-"*60)
    print(f"💵 Initial Total Principal Invested: ₹{initial_total_capital:,.2f}")
    print(f"🚀 Final Portfolio Value Today:     ₹{final_total_value:,.2f}")
    print(f"📊 Net Absolute Return:            ₹{overall_pnl:+,.2f} ({overall_return_pct:+.2f})")
    print("="*60)

class CustomArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        sys.stderr.write(f"\n❌ Parameter Error: {message}\n")
        self.print_help()
        sys.exit(2)

def show_usage_guide():
    return """
💡 RUN CODES SAMPLER:
------------------------------------------------------------
1. Run with 12 months window holding 10 stocks starting from 2024:
   python strategy.py --start_date 2024-01-01 --top_gainer_for_period 12 --top_n 10
2. Run default setup (2024-01-01, 12 Months, 7 Stocks):
   python strategy.py
------------------------------------------------------------
"""

if __name__ == "__main__":
    parser = CustomArgumentParser(
        description="📈 NIFTY Midcap 150 Strategy Backtester",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=show_usage_guide()
    )
    parser.add_argument("--start_date", type=str, default="2024-01-01", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--top_gainer_for_period", type=int, default=12, help="Evaluation window in months")
    parser.add_argument("--top_n", type=int, default=7, help="Portfolio size")
    args = parser.parse_args()
    try:
        run_momentum_strategy(args.start_date, args.top_gainer_for_period, args.top_n)
    except Exception as err:
        print(f"❌ Strategy Execution Failure: {err}")

