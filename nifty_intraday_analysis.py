#!/usr/bin/env python3
"""
NIFTY Intraday Analysis - Comprehensive Research Script
Analyzes 1-minute candle data across 20 trading days.
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime, time, timedelta
from collections import defaultdict

DATA_DIR = "/root/workspace/Trading/data/nifty_index/"

# ─────────────────────────────────────────────────────────────
# DATA LOADING
# ─────────────────────────────────────────────────────────────
def load_all_days():
    """Load all minute-level CSV files, return dict of date_str -> DataFrame."""
    files = sorted([f for f in os.listdir(DATA_DIR) if f.endswith("_minute.csv")])
    days = {}
    for f in files:
        date_str = f.replace("_minute.csv", "")
        df = pd.read_csv(os.path.join(DATA_DIR, f), parse_dates=["date"])
        # Ensure sorted by time
        df = df.sort_values("date").reset_index(drop=True)
        # Extract time component for easier filtering
        df["time"] = df["date"].dt.strftime("%H:%M")
        df["hour"] = df["date"].dt.hour
        df["minute"] = df["date"].dt.minute
        days[date_str] = df
    return days


def get_candles_in_range(df, start_time_str, end_time_str):
    """Return candles where time >= start and time < end (exclusive of end)."""
    return df[(df["time"] >= start_time_str) & (df["time"] < end_time_str)]


def get_candles_from(df, start_time_str):
    """Return candles from start_time_str onwards."""
    return df[df["time"] >= start_time_str]


def get_candles_until(df, end_time_str):
    """Return candles up to and including end_time_str."""
    return df[df["time"] <= end_time_str]


# ─────────────────────────────────────────────────────────────
# UTILITY: Print section headers
# ─────────────────────────────────────────────────────────────
def print_header(title):
    width = 100
    print("\n" + "=" * width)
    print(f"  {title}")
    print("=" * width)


def print_subheader(title):
    print(f"\n--- {title} ---")


# ─────────────────────────────────────────────────────────────
# 1. OPENING RANGE BREAKOUT (ORB)
# ─────────────────────────────────────────────────────────────
def analyze_orb(days):
    print_header("1. OPENING RANGE BREAKOUT (ORB) ANALYSIS")

    orb_configs = [
        ("5-min ORB  (9:15-9:20)", "09:15", "09:20"),
        ("15-min ORB (9:15-9:30)", "09:15", "09:30"),
        ("30-min ORB (9:15-9:45)", "09:15", "09:45"),
    ]

    SL_POINTS = 10
    TRAIL_POINTS = 10

    for config_name, orb_start, orb_end in orb_configs:
        print_subheader(config_name)
        print(f"  SL: {SL_POINTS} pts from entry | Trail: {TRAIL_POINTS} pts (lock in once {TRAIL_POINTS}+ pts profit)")
        print(f"  {'Date':<14} {'ORB High':>10} {'ORB Low':>10} {'Signal':>8} {'Entry':>10} "
              f"{'Exit':>10} {'Exit Time':<8} {'P&L':>8} {'Reason':<20}")
        print(f"  {'-'*14} {'-'*10} {'-'*10} {'-'*8} {'-'*10} {'-'*10} {'-'*8} {'-'*8} {'-'*20}")

        total_pnl = 0
        wins = 0
        losses = 0
        no_signal = 0
        trades = 0
        day_results = []

        sorted_dates = sorted(days.keys())
        for date_str in sorted_dates:
            df = days[date_str]

            # ORB range candles
            orb_candles = get_candles_in_range(df, orb_start, orb_end)
            if orb_candles.empty:
                continue

            orb_high = orb_candles["high"].max()
            orb_low = orb_candles["low"].min()

            # Candles after ORB period
            post_orb = df[df["time"] >= orb_end].copy()
            # Cut off at 15:20 for exit
            post_orb = post_orb[post_orb["time"] <= "15:20"]

            if post_orb.empty:
                continue

            signal = None
            entry_price = None
            entry_time = None

            # Check for breakout
            for _, candle in post_orb.iterrows():
                if candle["high"] > orb_high and signal is None:
                    signal = "CE"
                    entry_price = orb_high  # entry at breakout level
                    entry_time = candle["time"]
                    break
                elif candle["low"] < orb_low and signal is None:
                    signal = "PE"
                    entry_price = orb_low
                    entry_time = candle["time"]
                    break

            if signal is None:
                no_signal += 1
                print(f"  {date_str:<14} {orb_high:>10.1f} {orb_low:>10.1f} {'NONE':>8} "
                      f"{'--':>10} {'--':>10} {'--':<8} {'0.0':>8} {'No breakout':<20}")
                continue

            # Simulate trade with SL and trailing SL
            trades += 1
            remaining = post_orb[post_orb["time"] >= entry_time]

            sl = SL_POINTS
            best_price = entry_price
            exit_price = None
            exit_time = None
            exit_reason = ""

            if signal == "CE":
                # Long side: bought CE at breakout above ORB high
                trailing_sl = entry_price - sl
                for _, candle in remaining.iterrows():
                    # Update best price (highest)
                    if candle["high"] > best_price:
                        best_price = candle["high"]
                    # Update trailing SL
                    profit = best_price - entry_price
                    if profit >= TRAIL_POINTS:
                        new_trail = best_price - TRAIL_POINTS
                        if new_trail > trailing_sl:
                            trailing_sl = new_trail
                    # Check if SL hit (candle low goes below trailing SL)
                    if candle["low"] <= trailing_sl:
                        exit_price = trailing_sl
                        exit_time = candle["time"]
                        exit_reason = "Trailing SL" if trailing_sl > (entry_price - sl) else "Initial SL"
                        break

                if exit_price is None:
                    # Exit at last candle close (EOD)
                    last_candle = remaining.iloc[-1]
                    exit_price = last_candle["close"]
                    exit_time = last_candle["time"]
                    exit_reason = "EOD exit"

                pnl = exit_price - entry_price

            else:  # PE
                # Short side: bought PE at breakout below ORB low
                trailing_sl = entry_price + sl
                for _, candle in remaining.iterrows():
                    if candle["low"] < best_price:
                        best_price = candle["low"]
                    profit = entry_price - best_price
                    if profit >= TRAIL_POINTS:
                        new_trail = best_price + TRAIL_POINTS
                        if new_trail < trailing_sl:
                            trailing_sl = new_trail
                    if candle["high"] >= trailing_sl:
                        exit_price = trailing_sl
                        exit_time = candle["time"]
                        exit_reason = "Trailing SL" if trailing_sl < (entry_price + sl) else "Initial SL"
                        break

                if exit_price is None:
                    last_candle = remaining.iloc[-1]
                    exit_price = last_candle["close"]
                    exit_time = last_candle["time"]
                    exit_reason = "EOD exit"

                pnl = entry_price - exit_price

            total_pnl += pnl
            if pnl > 0:
                wins += 1
            else:
                losses += 1

            day_results.append(pnl)

            print(f"  {date_str:<14} {orb_high:>10.1f} {orb_low:>10.1f} {signal:>8} {entry_price:>10.1f} "
                  f"{exit_price:>10.1f} {exit_time:<8} {pnl:>+8.1f} {exit_reason:<20}")

        # Summary
        print(f"\n  SUMMARY for {config_name}:")
        print(f"  Total trades: {trades} | Wins: {wins} | Losses: {losses} | No signal: {no_signal}")
        if trades > 0:
            print(f"  Win rate: {wins/trades*100:.1f}%")
            print(f"  Total P&L: {total_pnl:+.1f} pts | Avg P&L per trade: {total_pnl/trades:+.1f} pts")
            if day_results:
                print(f"  Best trade: {max(day_results):+.1f} pts | Worst trade: {min(day_results):+.1f} pts")
                print(f"  Avg winner: {np.mean([x for x in day_results if x > 0]):+.1f} pts" if wins > 0 else "")
                print(f"  Avg loser: {np.mean([x for x in day_results if x <= 0]):+.1f} pts" if losses > 0 else "")


# ─────────────────────────────────────────────────────────────
# 2. FIRST CANDLE DIRECTION PREDICTION
# ─────────────────────────────────────────────────────────────
def analyze_first_candle(days):
    print_header("2. FIRST CANDLE (9:15) DIRECTION vs DAY DIRECTION")

    print(f"  {'Date':<14} {'9:15 O':>10} {'9:15 C':>10} {'9:15 H':>10} {'9:15 L':>10} "
          f"{'1st Dir':>8} {'Day Close':>10} {'Day Dir':>8} {'Match?':>8} {'Close>9:15H?':>13}")
    print(f"  {'-'*14} {'-'*10} {'-'*10} {'-'*10} {'-'*10} {'-'*8} {'-'*10} {'-'*8} {'-'*8} {'-'*13}")

    match_count = 0
    close_above_high = 0
    close_below_low = 0
    bullish_predict_count = 0
    bearish_predict_count = 0
    total = 0

    sorted_dates = sorted(days.keys())
    for date_str in sorted_dates:
        df = days[date_str]

        first_candle = df[df["time"] == "09:15"]
        if first_candle.empty:
            continue

        fc = first_candle.iloc[0]
        fc_open = fc["open"]
        fc_close = fc["close"]
        fc_high = fc["high"]
        fc_low = fc["low"]

        day_close = df.iloc[-1]["close"]
        day_open = fc_open

        fc_bullish = fc_close > fc_open
        fc_bearish = fc_close < fc_open
        fc_dir = "BULL" if fc_bullish else ("BEAR" if fc_bearish else "DOJI")

        day_bullish = day_close > day_open
        day_dir = "BULL" if day_bullish else "BEAR"

        # Match: first candle direction matches day direction
        match = (fc_bullish and day_bullish) or (fc_bearish and not day_bullish)
        if fc_close == fc_open:
            match = False

        # Close above 9:15 high (for bullish first candle)
        above_high = day_close > fc_high
        below_low = day_close < fc_low

        total += 1
        if match:
            match_count += 1
        if fc_bullish:
            bullish_predict_count += 1
            if above_high:
                close_above_high += 1
        elif fc_bearish:
            bearish_predict_count += 1
            if below_low:
                close_below_low += 1

        above_str = "YES" if (fc_bullish and above_high) or (fc_bearish and below_low) else "NO"

        print(f"  {date_str:<14} {fc_open:>10.1f} {fc_close:>10.1f} {fc_high:>10.1f} {fc_low:>10.1f} "
              f"{fc_dir:>8} {day_close:>10.1f} {day_dir:>8} {'YES' if match else 'NO':>8} {above_str:>13}")

    print(f"\n  SUMMARY:")
    print(f"  Total days: {total}")
    print(f"  First candle predicts day direction: {match_count}/{total} = {match_count/total*100:.1f}%")
    print(f"  Bullish 9:15 candles: {bullish_predict_count} | Day closed above 9:15 high: {close_above_high} ({close_above_high/max(bullish_predict_count,1)*100:.1f}%)")
    print(f"  Bearish 9:15 candles: {bearish_predict_count} | Day closed below 9:15 low: {close_below_low} ({close_below_low/max(bearish_predict_count,1)*100:.1f}%)")


# ─────────────────────────────────────────────────────────────
# 3. VWAP ANALYSIS
# ─────────────────────────────────────────────────────────────
def analyze_vwap(days):
    print_header("3. VWAP CROSSOVER ANALYSIS")
    print("  Strategy: Buy CE on cross above VWAP, exit on cross below VWAP or 15:20")
    print("            Buy PE on cross below VWAP, exit on cross above VWAP or 15:20")

    # Since volume is 0 for index, use typical price as proxy
    # VWAP = cumulative(TP * vol) / cumulative(vol)
    # With 0 volume, we'll use equal-weighted VWAP (cumulative typical price / count)
    print("  Note: Volume is 0 for index data, using equal-weighted VWAP (cumulative avg of typical price)\n")

    print(f"  {'Date':<14} {'#CE Trades':>10} {'CE P&L':>10} {'#PE Trades':>10} {'PE P&L':>10} {'Day Total':>10}")
    print(f"  {'-'*14} {'-'*10} {'-'*10} {'-'*10} {'-'*10} {'-'*10}")

    grand_ce_pnl = 0
    grand_pe_pnl = 0
    grand_ce_trades = 0
    grand_pe_trades = 0
    all_ce_trades = []
    all_pe_trades = []

    sorted_dates = sorted(days.keys())
    for date_str in sorted_dates:
        df = days[date_str].copy()

        # Calculate VWAP (equal-weighted since volume=0)
        df["typical_price"] = (df["high"] + df["low"] + df["close"]) / 3
        df["cum_tp"] = df["typical_price"].cumsum()
        df["cum_count"] = range(1, len(df) + 1)
        df["vwap"] = df["cum_tp"] / df["cum_count"]

        # Determine position: above or below VWAP
        df["above_vwap"] = df["close"] > df["vwap"]

        # Cut off at 15:20
        trade_df = df[df["time"] <= "15:20"].copy()
        if len(trade_df) < 2:
            continue

        # Detect crossovers
        trade_df["prev_above"] = trade_df["above_vwap"].shift(1)
        trade_df = trade_df.dropna(subset=["prev_above"])

        ce_pnl_day = 0
        pe_pnl_day = 0
        ce_trades_day = 0
        pe_trades_day = 0

        in_ce = False
        in_pe = False
        entry_price = 0

        for _, row in trade_df.iterrows():
            cross_above = (not row["prev_above"]) and row["above_vwap"]
            cross_below = row["prev_above"] and (not row["above_vwap"])

            # Exit existing trades on crossover
            if in_ce and cross_below:
                pnl = row["close"] - entry_price
                ce_pnl_day += pnl
                all_ce_trades.append(pnl)
                in_ce = False

            if in_pe and cross_above:
                pnl = entry_price - row["close"]
                pe_pnl_day += pnl
                all_pe_trades.append(pnl)
                in_pe = False

            # Enter new trades on crossover
            if cross_above and not in_ce:
                in_ce = True
                entry_price = row["close"]
                ce_trades_day += 1

            if cross_below and not in_pe:
                in_pe = True
                entry_price = row["close"]
                pe_trades_day += 1

        # EOD exit for open positions at last available candle
        last_row = trade_df.iloc[-1]
        if in_ce:
            pnl = last_row["close"] - entry_price
            ce_pnl_day += pnl
            all_ce_trades.append(pnl)
        if in_pe:
            pnl = entry_price - last_row["close"]
            pe_pnl_day += pnl
            all_pe_trades.append(pnl)

        grand_ce_pnl += ce_pnl_day
        grand_pe_pnl += pe_pnl_day
        grand_ce_trades += ce_trades_day
        grand_pe_trades += pe_trades_day

        day_total = ce_pnl_day + pe_pnl_day
        print(f"  {date_str:<14} {ce_trades_day:>10} {ce_pnl_day:>+10.1f} {pe_trades_day:>10} {pe_pnl_day:>+10.1f} {day_total:>+10.1f}")

    print(f"\n  SUMMARY:")
    print(f"  CE Trades: {grand_ce_trades} | Total CE P&L: {grand_ce_pnl:+.1f} pts | Avg: {grand_ce_pnl/max(grand_ce_trades,1):+.1f} pts")
    print(f"  PE Trades: {grand_pe_trades} | Total PE P&L: {grand_pe_pnl:+.1f} pts | Avg: {grand_pe_pnl/max(grand_pe_trades,1):+.1f} pts")
    print(f"  Combined: {grand_ce_trades+grand_pe_trades} trades | P&L: {grand_ce_pnl+grand_pe_pnl:+.1f} pts")
    if all_ce_trades:
        ce_wins = [t for t in all_ce_trades if t > 0]
        ce_losses = [t for t in all_ce_trades if t <= 0]
        print(f"  CE Win rate: {len(ce_wins)}/{len(all_ce_trades)} = {len(ce_wins)/len(all_ce_trades)*100:.1f}%")
        if ce_wins:
            print(f"    Avg CE winner: {np.mean(ce_wins):+.1f} | Avg CE loser: {np.mean(ce_losses):+.1f}" if ce_losses else f"    Avg CE winner: {np.mean(ce_wins):+.1f}")
    if all_pe_trades:
        pe_wins = [t for t in all_pe_trades if t > 0]
        pe_losses = [t for t in all_pe_trades if t <= 0]
        print(f"  PE Win rate: {len(pe_wins)}/{len(all_pe_trades)} = {len(pe_wins)/len(all_pe_trades)*100:.1f}%")
        if pe_wins:
            print(f"    Avg PE winner: {np.mean(pe_wins):+.1f} | Avg PE loser: {np.mean(pe_losses):+.1f}" if pe_losses else f"    Avg PE winner: {np.mean(pe_wins):+.1f}")


# ─────────────────────────────────────────────────────────────
# 4. MEAN REVERSION AFTER 50+ PT MOVE
# ─────────────────────────────────────────────────────────────
def analyze_mean_reversion(days):
    print_header("4. MEAN REVERSION AFTER 50+ POINT MOVE IN FIRST 30 MINUTES")
    print("  Check: If Nifty moves 50+ pts from day open within first 30 min, does it revert?\n")

    THRESHOLD = 50
    print(f"  {'Date':<14} {'Open':>10} {'Move Dir':>10} {'Extreme':>10} {'Move Pts':>10} "
          f"{'Reverted?':>10} {'Revert Amt':>10} {'Revert %':>10} {'Revert Time':>12} {'Day Close':>10}")
    print(f"  {'-'*14} {'-'*10} {'-'*10} {'-'*10} {'-'*10} {'-'*10} {'-'*10} {'-'*10} {'-'*12} {'-'*10}")

    total_moves = 0
    reverted_count = 0
    full_revert_count = 0
    revert_amounts = []
    revert_pcts = []

    sorted_dates = sorted(days.keys())
    for date_str in sorted_dates:
        df = days[date_str]

        day_open = df.iloc[0]["open"]
        first_30 = get_candles_in_range(df, "09:15", "09:45")

        if first_30.empty:
            continue

        # Find max move from open in first 30 min
        max_high = first_30["high"].max()
        min_low = first_30["low"].min()

        up_move = max_high - day_open
        down_move = day_open - min_low

        # Determine dominant move
        if up_move >= THRESHOLD and up_move >= down_move:
            move_dir = "UP"
            extreme = max_high
            move_pts = up_move
        elif down_move >= THRESHOLD and down_move > up_move:
            move_dir = "DOWN"
            extreme = min_low
            move_pts = down_move
        else:
            continue  # No 50+ pt move

        total_moves += 1

        # Check reversion after the first 30 min
        after_30 = df[df["time"] >= "09:45"]

        if move_dir == "UP":
            # Reversion = price comes back towards open (downward from extreme)
            min_after = after_30["low"].min()
            revert_amount = extreme - min_after
            revert_pct = (revert_amount / move_pts * 100) if move_pts > 0 else 0
            reverted = revert_amount > (move_pts * 0.5)  # >50% reversion
            full_revert = min_after <= day_open

            # Find time of max reversion
            min_idx = after_30["low"].idxmin()
            revert_time = after_30.loc[min_idx, "time"] if min_idx in after_30.index else "--"
        else:
            max_after = after_30["high"].max()
            revert_amount = max_after - extreme
            revert_pct = (revert_amount / move_pts * 100) if move_pts > 0 else 0
            reverted = revert_amount > (move_pts * 0.5)
            full_revert = max_after >= day_open

            max_idx = after_30["high"].idxmax()
            revert_time = after_30.loc[max_idx, "time"] if max_idx in after_30.index else "--"

        if reverted:
            reverted_count += 1
        if full_revert:
            full_revert_count += 1

        revert_amounts.append(revert_amount)
        revert_pcts.append(revert_pct)

        day_close = df.iloc[-1]["close"]

        print(f"  {date_str:<14} {day_open:>10.1f} {move_dir:>10} {extreme:>10.1f} {move_pts:>10.1f} "
              f"{'YES(>50%)' if reverted else 'NO':>10} {revert_amount:>10.1f} {revert_pct:>9.1f}% {revert_time:>12} {day_close:>10.1f}")

    print(f"\n  SUMMARY:")
    print(f"  Days with 50+ pt move in first 30 min: {total_moves}/{len(days)}")
    if total_moves > 0:
        print(f"  Reverted >50% of move: {reverted_count}/{total_moves} = {reverted_count/total_moves*100:.1f}%")
        print(f"  Full revert (back to open): {full_revert_count}/{total_moves} = {full_revert_count/total_moves*100:.1f}%")
        print(f"  Avg reversion amount: {np.mean(revert_amounts):.1f} pts")
        print(f"  Avg reversion %: {np.mean(revert_pcts):.1f}%")
        print(f"  Median reversion %: {np.median(revert_pcts):.1f}%")


# ─────────────────────────────────────────────────────────────
# 5. TREND DAYS vs RANGE DAYS
# ─────────────────────────────────────────────────────────────
def analyze_trend_vs_range(days):
    print_header("5. TREND DAYS vs RANGE DAYS CLASSIFICATION")
    print("  Trend day: |Open-to-Close| > 100 pts")
    print("  Range day: |Open-to-Close| < 50 pts")
    print("  In-between: 50-100 pts\n")

    TREND_THRESHOLD = 100
    RANGE_THRESHOLD = 50

    print(f"  {'Date':<14} {'Open':>10} {'Close':>10} {'O-to-C':>10} {'Day High':>10} {'Day Low':>10} "
          f"{'Range':>10} {'Type':<12} {'Best ORB':>10}")
    print(f"  {'-'*14} {'-'*10} {'-'*10} {'-'*10} {'-'*10} {'-'*10} {'-'*10} {'-'*12} {'-'*10}")

    trend_days = []
    range_days = []
    mid_days = []

    sorted_dates = sorted(days.keys())
    for date_str in sorted_dates:
        df = days[date_str]

        day_open = df.iloc[0]["open"]
        day_close = df.iloc[-1]["close"]
        day_high = df["high"].max()
        day_low = df["low"].min()
        day_range = day_high - day_low
        otc = day_close - day_open

        abs_otc = abs(otc)

        if abs_otc > TREND_THRESHOLD:
            day_type = "TREND"
            trend_days.append((date_str, otc, day_range, df))
        elif abs_otc < RANGE_THRESHOLD:
            day_type = "RANGE"
            range_days.append((date_str, otc, day_range, df))
        else:
            day_type = "MODERATE"
            mid_days.append((date_str, otc, day_range, df))

        # Simulate simple directional entry strategies for trend days
        # Best entry: earliest directional entry that captures the move
        if abs_otc > TREND_THRESHOLD:
            direction = "LONG" if otc > 0 else "SHORT"
            # Entry at open, hold till close
            best_orb_pnl = abs(otc)
        else:
            direction = ""
            best_orb_pnl = 0

        print(f"  {date_str:<14} {day_open:>10.1f} {day_close:>10.1f} {otc:>+10.1f} {day_high:>10.1f} {day_low:>10.1f} "
              f"{day_range:>10.1f} {day_type:<12} {best_orb_pnl:>+10.1f}")

    # Detailed trend day analysis
    print_subheader("Trend Day Analysis")
    print(f"  Trend days: {len(trend_days)} | Range days: {len(range_days)} | Moderate: {len(mid_days)}")

    if trend_days:
        print(f"\n  On TREND days, testing entry strategies:")
        print(f"  {'Strategy':<40} {'Avg P&L':>10} {'Win Rate':>10} {'Total P&L':>10}")
        print(f"  {'-'*40} {'-'*10} {'-'*10} {'-'*10}")

        # Strategy 1: Enter at 9:30 in direction of first 15 min
        strat_results = {"9:30 (15m dir)": [], "9:45 (30m dir)": [], "10:00 (45m dir)": []}

        for date_str, otc, day_range, df in trend_days:
            day_open = df.iloc[0]["open"]
            day_close = df.iloc[-1]["close"]

            for entry_time_str, strat_name in [("09:30", "9:30 (15m dir)"), ("09:45", "9:45 (30m dir)"), ("10:00", "10:00 (45m dir)")]:
                entry_candles = df[df["time"] == entry_time_str]
                if entry_candles.empty:
                    continue
                entry_price = entry_candles.iloc[0]["open"]
                # Direction from open to entry
                direction = 1 if entry_price > day_open else -1
                pnl = direction * (day_close - entry_price)
                strat_results[strat_name].append(pnl)

        for strat_name, results in strat_results.items():
            if results:
                avg_pnl = np.mean(results)
                win_rate = len([r for r in results if r > 0]) / len(results) * 100
                total = sum(results)
                print(f"  {strat_name:<40} {avg_pnl:>+10.1f} {win_rate:>9.1f}% {total:>+10.1f}")

    # Range day analysis
    if range_days:
        print(f"\n  On RANGE days (<50pt O-to-C), simulating mean-reversion (fade 30pt moves with 15pt target, 20pt SL):")
        range_trade_pnl = []
        for date_str, otc, day_range, df in range_days:
            day_open = df.iloc[0]["open"]
            # Look for 30pt move from open, then fade
            for _, candle in df.iterrows():
                if candle["high"] - day_open >= 30:
                    # Short entry
                    entry = candle["high"]
                    remaining = df[df["time"] > candle["time"]]
                    for _, rc in remaining.iterrows():
                        if rc["low"] <= entry - 15:
                            range_trade_pnl.append(15)
                            break
                        if rc["high"] >= entry + 20:
                            range_trade_pnl.append(-20)
                            break
                    break
                elif day_open - candle["low"] >= 30:
                    entry = candle["low"]
                    remaining = df[df["time"] > candle["time"]]
                    for _, rc in remaining.iterrows():
                        if rc["high"] >= entry + 15:
                            range_trade_pnl.append(15)
                            break
                        if rc["low"] <= entry - 20:
                            range_trade_pnl.append(-20)
                            break
                    break

        if range_trade_pnl:
            wins = len([p for p in range_trade_pnl if p > 0])
            print(f"  Trades taken: {len(range_trade_pnl)} | Wins: {wins} | Win rate: {wins/len(range_trade_pnl)*100:.1f}%")
            print(f"  Total P&L: {sum(range_trade_pnl):+.1f} | Avg: {np.mean(range_trade_pnl):+.1f}")
        else:
            print(f"  No qualifying fade trades found on range days.")
        print(f"\n  Recommendation: On range days, frequent VWAP crosses cause whipsaws. Avoid directional trades or use tight targets.")


# ─────────────────────────────────────────────────────────────
# 6. BEST TIME TO ENTER (MAX FAVORABLE EXCURSION BY 15-MIN SLOT)
# ─────────────────────────────────────────────────────────────
def analyze_best_entry_time(days):
    print_header("6. BEST TIME TO ENTER - MAX FAVORABLE EXCURSION BY 15-MIN SLOT")
    print("  For each 15-min slot, enter in the direction of move from 9:15 open.")
    print("  MFE = max profit before a 30pt adverse move occurs.\n")

    ADVERSE_THRESHOLD = 30

    # 15-min slots from 9:15 to 15:00
    slots = []
    h, m = 9, 15
    while h < 15 or (h == 15 and m <= 0):
        slots.append(f"{h:02d}:{m:02d}")
        m += 15
        if m >= 60:
            m -= 60
            h += 1

    slot_mfe = defaultdict(list)
    slot_mae = defaultdict(list)
    slot_pnl_30pt_sl = defaultdict(list)

    sorted_dates = sorted(days.keys())
    for date_str in sorted_dates:
        df = days[date_str]
        day_open = df.iloc[0]["open"]
        day_close = df.iloc[-1]["close"]
        day_dir = 1 if day_close > day_open else -1  # actual day direction

        for slot_time in slots:
            slot_candles = df[df["time"] == slot_time]
            if slot_candles.empty:
                continue

            entry_price = slot_candles.iloc[0]["close"]

            # Determine direction: from 9:15 open to current price
            direction = 1 if entry_price >= day_open else -1

            # Look forward from entry
            remaining = df[df["time"] > slot_time]
            remaining = remaining[remaining["time"] <= "15:25"]

            if remaining.empty:
                continue

            mfe = 0  # max favorable excursion
            mae = 0  # max adverse excursion before 30pt adverse
            mfe_before_adverse = 0

            for _, candle in remaining.iterrows():
                if direction == 1:
                    favorable = candle["high"] - entry_price
                    adverse = entry_price - candle["low"]
                else:
                    favorable = entry_price - candle["low"]
                    adverse = candle["high"] - entry_price

                if favorable > mfe:
                    mfe = favorable

                if adverse > mae:
                    mae = adverse

                # Check if 30pt adverse move happened
                if adverse >= ADVERSE_THRESHOLD:
                    mfe_before_adverse = mfe
                    break
            else:
                # No adverse move hit
                mfe_before_adverse = mfe

            slot_mfe[slot_time].append(mfe_before_adverse)
            slot_mae[slot_time].append(mae)

    # Print results
    print(f"  {'Slot':<8} {'Avg MFE':>10} {'Med MFE':>10} {'Avg MAE':>10} {'MFE/MAE':>10} {'MFE>50':>8} {'Samples':>8}")
    print(f"  {'-'*8} {'-'*10} {'-'*10} {'-'*10} {'-'*10} {'-'*8} {'-'*8}")

    best_slot = ""
    best_mfe = 0

    for slot_time in slots:
        if slot_time not in slot_mfe or not slot_mfe[slot_time]:
            continue
        mfes = slot_mfe[slot_time]
        maes = slot_mae[slot_time]
        avg_mfe = np.mean(mfes)
        med_mfe = np.median(mfes)
        avg_mae = np.mean(maes)
        ratio = avg_mfe / avg_mae if avg_mae > 0 else float('inf')
        mfe_gt_50 = len([m for m in mfes if m > 50])
        n = len(mfes)

        if avg_mfe > best_mfe:
            best_mfe = avg_mfe
            best_slot = slot_time

        print(f"  {slot_time:<8} {avg_mfe:>10.1f} {med_mfe:>10.1f} {avg_mae:>10.1f} {ratio:>10.2f} {mfe_gt_50:>8} {n:>8}")

    print(f"\n  BEST ENTRY SLOT: {best_slot} with avg MFE of {best_mfe:.1f} pts before 30pt adverse move")
    print(f"  Note: Earlier slots capture more of the day's move but have higher MAE.")
    print(f"        Later slots have less opportunity but better MFE/MAE ratio if trend is established.")


# ─────────────────────────────────────────────────────────────
# 7. GAP ANALYSIS
# ─────────────────────────────────────────────────────────────
def analyze_gaps(days):
    print_header("7. GAP ANALYSIS (Opening Gap vs Previous Day Close)")

    sorted_dates = sorted(days.keys())

    print(f"  {'Date':<14} {'Prev Close':>10} {'Today Open':>10} {'Gap':>10} {'Gap%':>8} "
          f"{'Filled?':>8} {'Fill Time':>10} {'Fill Min':>10} {'Day Close':>10} {'Gap Fade?':>10}")
    print(f"  {'-'*14} {'-'*10} {'-'*10} {'-'*10} {'-'*8} {'-'*8} {'-'*10} {'-'*10} {'-'*10} {'-'*10}")

    total_gaps = 0
    gaps_filled = 0
    gaps_faded = 0  # gap direction was faded by close
    fill_times = []
    gap_up_count = 0
    gap_down_count = 0
    gap_up_filled = 0
    gap_down_filled = 0

    for i in range(1, len(sorted_dates)):
        prev_date = sorted_dates[i - 1]
        curr_date = sorted_dates[i]

        prev_df = days[prev_date]
        curr_df = days[curr_date]

        prev_close = prev_df.iloc[-1]["close"]
        curr_open = curr_df.iloc[0]["open"]

        gap = curr_open - prev_close
        gap_pct = (gap / prev_close) * 100

        if abs(gap) < 5:
            # Skip negligible gaps
            print(f"  {curr_date:<14} {prev_close:>10.1f} {curr_open:>10.1f} {gap:>+10.1f} {gap_pct:>+7.2f}% "
                  f"{'--':>8} {'--':>10} {'--':>10} {curr_df.iloc[-1]['close']:>10.1f} {'--':>10}")
            continue

        total_gaps += 1
        is_gap_up = gap > 0

        if is_gap_up:
            gap_up_count += 1
        else:
            gap_down_count += 1

        # Check if gap filled (price touches previous close level)
        filled = False
        fill_time_str = "--"
        fill_minutes = "--"

        for j, (_, candle) in enumerate(curr_df.iterrows()):
            if is_gap_up and candle["low"] <= prev_close:
                filled = True
                fill_time_str = candle["time"]
                fill_minutes = str(j)  # minutes from open
                break
            elif not is_gap_up and candle["high"] >= prev_close:
                filled = True
                fill_time_str = candle["time"]
                fill_minutes = str(j)
                break

        if filled:
            gaps_filled += 1
            fill_times.append(int(fill_minutes))
            if is_gap_up:
                gap_up_filled += 1
            else:
                gap_down_filled += 1

        # Did the gap direction fade? (close opposite to gap direction)
        day_close = curr_df.iloc[-1]["close"]
        gap_faded = (is_gap_up and day_close < curr_open) or (not is_gap_up and day_close > curr_open)
        if gap_faded:
            gaps_faded += 1

        print(f"  {curr_date:<14} {prev_close:>10.1f} {curr_open:>10.1f} {gap:>+10.1f} {gap_pct:>+7.2f}% "
              f"{'YES' if filled else 'NO':>8} {fill_time_str:>10} {fill_minutes:>10} {day_close:>10.1f} "
              f"{'YES' if gap_faded else 'NO':>10}")

    print(f"\n  SUMMARY:")
    print(f"  Total significant gaps (>5 pts): {total_gaps}")
    if total_gaps > 0:
        print(f"  Gaps filled: {gaps_filled}/{total_gaps} = {gaps_filled/total_gaps*100:.1f}%")
        print(f"  Gap-up filled: {gap_up_filled}/{max(gap_up_count,1)} = {gap_up_filled/max(gap_up_count,1)*100:.1f}%")
        print(f"  Gap-down filled: {gap_down_filled}/{max(gap_down_count,1)} = {gap_down_filled/max(gap_down_count,1)*100:.1f}%")
        print(f"  Gap faded (close opposite to gap): {gaps_faded}/{total_gaps} = {gaps_faded/total_gaps*100:.1f}%")
        if fill_times:
            print(f"  Avg fill time: {np.mean(fill_times):.0f} min from open")
            print(f"  Median fill time: {np.median(fill_times):.0f} min from open")
            print(f"  Fastest fill: {min(fill_times)} min | Slowest fill: {max(fill_times)} min")


# ─────────────────────────────────────────────────────────────
# GRAND SUMMARY
# ─────────────────────────────────────────────────────────────
def print_grand_summary(days):
    print_header("GRAND SUMMARY - KEY FINDINGS")

    sorted_dates = sorted(days.keys())
    n = len(sorted_dates)

    # Basic stats
    daily_moves = []
    daily_ranges = []
    for date_str in sorted_dates:
        df = days[date_str]
        o = df.iloc[0]["open"]
        c = df.iloc[-1]["close"]
        h = df["high"].max()
        l = df["low"].min()
        daily_moves.append(c - o)
        daily_ranges.append(h - l)

    print(f"\n  Dataset: {n} trading days ({sorted_dates[0]} to {sorted_dates[-1]})")
    print(f"  Avg daily O-to-C move: {np.mean(np.abs(daily_moves)):.1f} pts (signed avg: {np.mean(daily_moves):+.1f})")
    print(f"  Avg daily range (H-L): {np.mean(daily_ranges):.1f} pts")
    print(f"  Max daily range: {max(daily_ranges):.1f} pts | Min: {min(daily_ranges):.1f} pts")

    bullish_days = len([m for m in daily_moves if m > 0])
    bearish_days = len([m for m in daily_moves if m < 0])
    print(f"  Bullish days: {bullish_days} | Bearish days: {bearish_days}")


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 100)
    print("  NIFTY INTRADAY RESEARCH ANALYSIS")
    print("  Data: 1-minute candles | Research only - not trading advice")
    print("=" * 100)

    days = load_all_days()
    print(f"\n  Loaded {len(days)} trading days of minute data.")
    print(f"  Date range: {min(days.keys())} to {max(days.keys())}")

    analyze_orb(days)
    analyze_first_candle(days)
    analyze_vwap(days)
    analyze_mean_reversion(days)
    analyze_trend_vs_range(days)
    analyze_best_entry_time(days)
    analyze_gaps(days)
    print_grand_summary(days)

    print("\n" + "=" * 100)
    print("  END OF ANALYSIS")
    print("=" * 100)
