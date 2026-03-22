"""Supertrend (Candle Close) — same entry signals as supertrend but exits at
5-min candle close when (high_since_entry - close) > sl_points.
No exchange SL order, no target/trailing."""

from datetime import datetime, timedelta

import pytz

from ..base_strategy import (
    BaseStrategy,
    StrategyContext,
    register_strategy,
    select_nifty_option,
)

IST = pytz.timezone("Asia/Kolkata")


def _compute_supertrend(candles, period, multiplier):
    """Compute Supertrend using Wilder's smoothed ATR.

    Returns (supertrend_values, directions) — parallel lists.
    supertrend_values[i] = None for the first `period` candles.
    directions[i] = 1 (bullish/price above) or -1 (bearish), 0 = not enough data.
    """
    n = len(candles)
    if n == 0:
        return [], []

    # True Range
    tr = [candles[0]["high"] - candles[0]["low"]]
    for i in range(1, n):
        tr.append(max(
            candles[i]["high"] - candles[i]["low"],
            abs(candles[i]["high"] - candles[i - 1]["close"]),
            abs(candles[i]["low"] - candles[i - 1]["close"]),
        ))

    if n < period:
        return [None] * n, [0] * n

    # ATR — Wilder's smoothing: first = SMA, then EMA-like
    atr = [0.0] * n
    atr[period - 1] = sum(tr[:period]) / period
    for i in range(period, n):
        atr[i] = (atr[i - 1] * (period - 1) + tr[i]) / period

    # Basic upper/lower bands
    basic_upper = [0.0] * n
    basic_lower = [0.0] * n
    for i in range(n):
        hl2 = (candles[i]["high"] + candles[i]["low"]) / 2
        basic_upper[i] = hl2 + multiplier * atr[i]
        basic_lower[i] = hl2 - multiplier * atr[i]

    # Final bands and supertrend
    final_upper = [0.0] * n
    final_lower = [0.0] * n
    supertrend = [None] * n
    direction = [0] * n

    start = period - 1
    final_upper[start] = basic_upper[start]
    final_lower[start] = basic_lower[start]

    if candles[start]["close"] > final_upper[start]:
        direction[start] = 1
        supertrend[start] = final_lower[start]
    else:
        direction[start] = -1
        supertrend[start] = final_upper[start]

    for i in range(start + 1, n):
        if basic_upper[i] < final_upper[i - 1] or candles[i - 1]["close"] > final_upper[i - 1]:
            final_upper[i] = basic_upper[i]
        else:
            final_upper[i] = final_upper[i - 1]

        if basic_lower[i] > final_lower[i - 1] or candles[i - 1]["close"] < final_lower[i - 1]:
            final_lower[i] = basic_lower[i]
        else:
            final_lower[i] = final_lower[i - 1]

        if direction[i - 1] == 1:
            direction[i] = -1 if candles[i]["close"] < final_lower[i] else 1
        else:
            direction[i] = 1 if candles[i]["close"] > final_upper[i] else -1

        supertrend[i] = final_lower[i] if direction[i] == 1 else final_upper[i]

    return supertrend, direction


def _aggregate_candles(candles, bar_minutes=5):
    """Aggregate 1-minute candles into N-minute OHLC bars."""
    bars = []
    i = 0
    while i < len(candles):
        cdt = candles[i]["date"]
        bar_key = (cdt.hour, (cdt.minute // bar_minutes) * bar_minutes)

        bar_indices = [i]
        j = i + 1
        while j < len(candles):
            jdt = candles[j]["date"]
            jkey = (jdt.hour, (jdt.minute // bar_minutes) * bar_minutes)
            if jkey == bar_key:
                bar_indices.append(j)
                j += 1
            else:
                break

        group = [candles[k] for k in bar_indices]
        bars.append({
            "date": group[0]["date"],
            "open": group[0]["open"],
            "high": max(c["high"] for c in group),
            "low": min(c["low"] for c in group),
            "close": group[-1]["close"],
            "_minute_indices": bar_indices,
        })
        i = j

    return bars


@register_strategy("supertrend_candle_close")
class SupertrendCandleStrategy(BaseStrategy):
    ST_PERIOD = 10
    ST_MULTIPLIER = 3.0
    BAR_MINUTES = 5

    use_exchange_sl = False
    use_targets = False
    candle_sl_minutes = 5

    @property
    def signal_based(self) -> bool:
        return True

    def select_instrument(self, ctx: StrategyContext, strategy_data: dict) -> dict | None:
        option_type = strategy_data.get("option_type", "CE")
        return select_nifty_option(ctx.client, option_type, ctx.settings.min_premium,
                                   ctx.settings.expiry_type)

    def initial_direction(self, strategy_data: dict, bias: str) -> str:
        return "BUY"

    # ── Live entry signal (same as supertrend) ─────────────────────────

    def get_entry_signal(self, client, settings, strategy_data) -> list | None:
        now = datetime.now(IST)
        today = now.date()

        refresh_key = f"{now.hour}:{now.minute}:{now.second // 30}"
        needs_refresh = strategy_data.get("_st_refresh") != refresh_key

        bar_min = self.BAR_MINUTES
        is_bar_open = (now.minute % bar_min == 0)
        check_key = f"{now.hour}:{now.minute}"
        is_signal_check = is_bar_open and strategy_data.get("_last_st_check") != check_key

        if not needs_refresh and not is_signal_check:
            return None

        strategy_data["_st_refresh"] = refresh_key

        ce_token = strategy_data.get("_ce_token")
        pe_token = strategy_data.get("_pe_token")
        signals = []

        for opt, token in [("CE", ce_token), ("PE", pe_token)]:
            if not token:
                continue

            try:
                from_date = today - timedelta(days=1)
                interval = f"{bar_min}minute"
                candles = client.kite.historical_data(token, from_date, today, interval)
            except Exception:
                continue
            if len(candles) < self.ST_PERIOD + 1:
                continue

            st_vals, st_dirs = _compute_supertrend(candles, self.ST_PERIOD, self.ST_MULTIPLIER)

            last_idx = len(candles) - 1
            st = st_vals[last_idx]
            if st is None:
                continue

            ltp_key = f"_ltp_{opt}"
            ltp = strategy_data.get(ltp_key, 0.0)
            candle_close = candles[last_idx]["close"]

            strategy_data[f"_st_value_{opt}"] = round(st, 2)
            strategy_data[f"_st_close_{opt}"] = round(ltp if ltp > 0 else candle_close, 2)
            strategy_data[f"_st_trend_{opt}"] = "UP" if (ltp or candle_close) > st else "DOWN"

            maxh_key = f"_max_high_since_entry_{opt}"

            entry_bar = None
            if st_dirs[last_idx] == 1:
                entry_bar = last_idx
                for bi in range(last_idx - 1, -1, -1):
                    if st_dirs[bi] == 1:
                        entry_bar = bi
                    else:
                        break

            max_high = 0.0
            if entry_bar is not None:
                for ci in range(entry_bar, last_idx):
                    max_high = max(max_high, candles[ci]["high"])
            strategy_data[maxh_key] = max_high
            strategy_data[f"_st_reentry_{opt}"] = round(max_high, 2)

            if not is_signal_check:
                continue
            if strategy_data.get(f"_active_{opt}"):
                continue

            prev_close = candles[last_idx - 1]["close"] if last_idx > 0 else candle_close
            prev_st = st_vals[last_idx - 1] if last_idx > 0 else st
            price = ltp if ltp > 0 else candle_close

            import logging
            _log = logging.getLogger("kite_wrapper.strategy")
            _log.info(
                f"[ST-CANDLE-{opt}] signal check: prev_close={prev_close:.2f}, "
                f"prev_ST={prev_st:.2f}, LTP={price:.2f}, ST={st:.2f}, "
                f"max_high={max_high:.2f}, entry_bar={entry_bar}"
            )

            if price < st:
                continue

            if prev_st is not None and prev_close < prev_st:
                signals.append({"direction": "BUY", "option_type": opt})
                continue

            if max_high > 0 and prev_close < max_high and price > max_high:
                signals.append({"direction": "BUY", "option_type": opt})

        if is_signal_check:
            strategy_data["_last_st_check"] = check_key

        return signals if signals else None

    # ── SL / Target ───────────────────────────────────────────────────

    def on_sl_hit(self, ctx: StrategyContext, strategy_data: dict) -> dict:
        opt = strategy_data.get("_sl_hit_option", "CE")
        strategy_data[f"_waiting_{opt}"] = True
        strategy_data[f"_active_{opt}"] = False
        return {"action": "stop"}

    def on_target_hit(self, ctx: StrategyContext, strategy_data: dict,
                      lots_remaining: int) -> dict:
        return {"action": "trail"}

    # ── Backtest ──────────────────────────────────────────────────────

    def precompute_signal_data(self, candles, warmup_candles=None):
        if warmup_candles:
            all_candles = warmup_candles + candles
            warmup_len = len(warmup_candles)
        else:
            all_candles = candles
            warmup_len = 0

        bars_5m = _aggregate_candles(all_candles, self.BAR_MINUTES)
        st_vals, st_dirs = _compute_supertrend(bars_5m, self.ST_PERIOD, self.ST_MULTIPLIER)

        all_per_minute_bar = [0] * len(all_candles)
        for bar_idx, bar in enumerate(bars_5m):
            for mi in bar["_minute_indices"]:
                all_per_minute_bar[mi] = bar_idx

        per_minute_bar = [all_per_minute_bar[warmup_len + i] for i in range(len(candles))]

        return {
            "bars_5m": bars_5m,
            "supertrend": st_vals,
            "direction": st_dirs,
            "per_minute_bar": per_minute_bar,
            "_in_buy_trend": False,
            "_st_entry_bar": None,
            "_max_high": 0.0,
            "_prev_bar_idx": -1,
        }

    def check_entry_signal_backtest(self, candles, signal_data, index, waiting):
        bar_idx = signal_data["per_minute_bar"][index]

        is_bar_close = (index == len(candles) - 1) or \
            (signal_data["per_minute_bar"][index + 1] != bar_idx)
        if not is_bar_close:
            return False

        if bar_idx == signal_data["_prev_bar_idx"]:
            return False
        signal_data["_prev_bar_idx"] = bar_idx

        st = signal_data["supertrend"][bar_idx]
        if st is None:
            return False

        close = candles[index]["close"]

        if signal_data["_in_buy_trend"] and signal_data["_st_entry_bar"] is not None:
            max_high = 0.0
            for bi in range(signal_data["_st_entry_bar"], bar_idx):
                max_high = max(max_high, signal_data["bars_5m"][bi]["high"])
            signal_data["_max_high"] = max_high

        if close < st:
            signal_data["_in_buy_trend"] = False
            signal_data["_st_entry_bar"] = None
            signal_data["_max_high"] = 0.0
            return False

        if not signal_data["_in_buy_trend"] and close > st:
            signal_data["_in_buy_trend"] = True
            signal_data["_st_entry_bar"] = bar_idx
            signal_data["_max_high"] = signal_data["bars_5m"][bar_idx]["high"]
            return True

        if signal_data["_in_buy_trend"] and waiting:
            if signal_data["_max_high"] > 0 and close > signal_data["_max_high"]:
                return True

        return False
