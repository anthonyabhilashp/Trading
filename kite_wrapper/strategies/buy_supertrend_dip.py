"""Buy Supertrend Dip — tracks both CE and PE options.
Uses previous day's 5-min candles for supertrend warmup.
Tracks rolling high across 5-min candles.
Entry conditions (all must be true on last completed 5-min candle):
  1. rolling_high - candle_low > sl_points (dip from high)
  2. candle_close - supertrend <= target_points (close is near supertrend)
  3. close > supertrend (bullish trend)
Exits when (high_since_entry - candle_low) > sl_points OR close < supertrend.
Can hold CE and PE simultaneously."""

import logging
from datetime import datetime, timedelta

import pytz

from ..base_strategy import (
    BaseStrategy,
    StrategyContext,
    register_strategy,
    select_nifty_option,
)

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

ST_PERIOD = 7       # Zerodha default
ST_MULTIPLIER = 3.0


def _compute_supertrend(candles, period, multiplier):
    """Compute Supertrend using Wilder's smoothed ATR.

    Returns (supertrend_values, directions) — parallel lists.
    supertrend_values[i] = None for the first `period` candles.
    directions[i] = 1 (bullish/price above) or -1 (bearish), 0 = not enough data.
    """
    n = len(candles)
    if n == 0:
        return [], []

    tr = [candles[0]["high"] - candles[0]["low"]]
    for i in range(1, n):
        tr.append(max(
            candles[i]["high"] - candles[i]["low"],
            abs(candles[i]["high"] - candles[i - 1]["close"]),
            abs(candles[i]["low"] - candles[i - 1]["close"]),
        ))

    if n < period:
        return [None] * n, [0] * n

    atr = [0.0] * n
    atr[period - 1] = sum(tr[:period]) / period
    for i in range(period, n):
        atr[i] = (atr[i - 1] * (period - 1) + tr[i]) / period

    basic_upper = [0.0] * n
    basic_lower = [0.0] * n
    for i in range(n):
        hl2 = (candles[i]["high"] + candles[i]["low"]) / 2
        basic_upper[i] = hl2 + multiplier * atr[i]
        basic_lower[i] = hl2 - multiplier * atr[i]

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


@register_strategy("buy_supertrend_dip_candle_close")
class BuySupertrendDipStrategy(BaseStrategy):

    use_exchange_sl = False
    use_targets = False
    candle_sl_minutes = 5
    candle_sl_use_prev_low = True
    candle_check_offset = 0  # check at exact :00, :05, :10...
    candle_check_delay_seconds = 0

    @property
    def signal_based(self) -> bool:
        return True

    def select_instrument(self, ctx: StrategyContext, strategy_data: dict) -> dict | None:
        option_type = strategy_data.get("option_type", "CE")
        return select_nifty_option(ctx.client, option_type, ctx.settings.min_premium,
                                   ctx.settings.expiry_type)

    def initial_direction(self, strategy_data: dict, bias: str) -> str:
        return "BUY"

    def get_entry_signal(self, client, settings, strategy_data) -> list | None:
        # Check at exact 5-min boundary (same as supertrend strategy)
        now = datetime.now(IST)
        today = now.date()

        is_bar_open = (now.minute % 5 == 0)
        check_key = f"{now.hour}:{now.minute}"
        is_signal_check = is_bar_open and strategy_data.get("_entry_check") != check_key

        if not is_signal_check:
            return None
        strategy_data["_entry_check"] = check_key

        # Store target_points for should_skip_exit to use
        strategy_data["_target_points_setting"] = settings.target_points

        signals = []

        for opt in ("CE", "PE"):
            ltp = strategy_data.get(f"_ltp_{opt}", 0.0)
            if ltp <= 0:
                continue

            # Skip if already in a position for this option
            if strategy_data.get(f"_active_{opt}"):
                continue

            # Reselect if price has deviated >20% from min_premium
            if settings.min_premium > 0:
                deviation = abs(ltp - settings.min_premium) / settings.min_premium
                if deviation > 0.20:
                    logger.info(
                        f"[ST-DIP-{opt}] LTP={ltp:.2f} deviated {deviation:.0%} from "
                        f"min_premium={settings.min_premium} — requesting reselect"
                    )
                    signals.append({"action": "reselect", "option_type": opt})
                    continue

            # Fetch previous day + today's 5-min candles for supertrend
            token = strategy_data.get(f"_{opt.lower()}_token")
            if not token:
                continue

            try:
                # Go back up to 5 days to cover weekends/holidays
                from_date = today - timedelta(days=5)
                candles = client.kite.historical_data(
                    token, from_date, today, "5minute"
                )
            except Exception as e:
                logger.error(f"[ST-DIP-{opt}] Failed to fetch 5-min candles: {e}")
                continue

            if len(candles) < ST_PERIOD + 2:
                continue

            # Compute supertrend on all candles
            st_vals, st_dirs = _compute_supertrend(candles, ST_PERIOD, ST_MULTIPLIER)

            # At exact :00 boundary, candles[-1] is the last completed candle
            last_idx = len(candles) - 1
            last_candle = candles[last_idx]
            st = st_vals[last_idx]
            if st is None:
                continue

            # Update rolling high from today's candles where supertrend is bullish
            # Only track highs since supertrend turned positive (dir=1)
            high_key = f"_rolling_high_{opt}"
            rolling_high = 0.0  # recalculate from scratch each time
            today_str = today.isoformat()
            for idx, c in enumerate(candles):
                cdt = c["date"]
                c_date = cdt.strftime("%Y-%m-%d") if hasattr(cdt, 'strftime') else str(cdt)[:10]
                if c_date != today_str:
                    continue
                if st_dirs[idx] == 1:  # only bullish candles
                    rolling_high = max(rolling_high, c["high"])
                else:
                    # Supertrend turned negative — reset rolling high
                    rolling_high = 0.0
            strategy_data[high_key] = rolling_high

            candle_low = last_candle["low"]
            close = last_candle["close"]
            dip_from_high = rolling_high - candle_low
            low_to_st = candle_low - st

            close_to_st = close - st

            # Log every check
            logger.info(
                f"[ST-DIP-{opt}] Check: rolling_high={rolling_high:.2f}, "
                f"candle_low={candle_low:.2f}, close={close:.2f}, ST={st:.2f}, "
                f"dip={dip_from_high:.2f}(need>{settings.sl_points}), "
                f"low-ST={low_to_st:.2f}(need<={settings.target_points}), "
                f"close-ST={close_to_st:.2f}(need<{settings.target_points}), "
                f"close>ST={close > st}"
            )

            # Entry conditions:
            # 1. Dip from rolling high: rolling_high - candle_low > sl_points
            if dip_from_high <= settings.sl_points:
                continue

            # 2. Low is near supertrend: low - supertrend <= target_points
            if low_to_st > settings.target_points:
                continue

            # 3. Bullish trend: close > supertrend
            if close <= st:
                continue

            # 4. Close is near supertrend: close - supertrend < target_points
            if close_to_st >= settings.target_points:
                continue

            logger.info(
                f"[ST-DIP-{opt}] Entry signal! dip={dip_from_high:.2f} > sl={settings.sl_points}, "
                f"low-ST={low_to_st:.2f} <= target={settings.target_points}, "
                f"close-ST={close_to_st:.2f} < target={settings.target_points}, "
                f"close={close:.2f} > ST={st:.2f}, entering at LTP={ltp:.2f}"
            )
            signals.append({"direction": "BUY", "option_type": opt})
            strategy_data[f"_entry_ltp_{opt}"] = ltp
            # Reset rolling high after entry
            strategy_data[high_key] = ltp

        return signals if signals else None

    def on_sl_hit(self, ctx: StrategyContext, strategy_data: dict) -> dict:
        opt = strategy_data.get("_sl_hit_option", "CE")
        strategy_data[f"_waiting_{opt}"] = True
        strategy_data[f"_active_{opt}"] = False
        logger.info(f"[ST-DIP-{opt}] SL hit — waiting for next signal")
        return {"action": "stop"}

    def _fetch_prev_candle_and_st(self, client, opt, strategy_data):
        """Fetch 5-min candles and return last completed candle + its supertrend."""
        now = datetime.now(IST)
        token = strategy_data.get(f"_{opt.lower()}_token")
        if not token:
            return None, None

        try:
            today = now.date()
            from_date = today - timedelta(days=5)
            candles = client.kite.historical_data(
                token, from_date, today, "5minute"
            )
        except Exception:
            return None, None

        if len(candles) < ST_PERIOD + 2:
            return None, None

        st_vals, _ = _compute_supertrend(candles, ST_PERIOD, ST_MULTIPLIER)
        # At exact boundary, candles[-1] is the last completed candle
        last_idx = len(candles) - 1
        st = st_vals[last_idx]
        if st is None:
            return None, None

        return candles[last_idx], st

    def should_skip_exit(self, client, opt, strategy_data):
        """Skip SL exit if candle low is near supertrend support (low - ST <= target)."""
        prev_candle, st = self._fetch_prev_candle_and_st(client, opt, strategy_data)
        if prev_candle is None:
            return False, ""

        low_to_st = prev_candle["low"] - st
        settings_target = strategy_data.get("_target_points_setting", 100)

        if low_to_st <= settings_target and prev_candle["close"] > st:
            return True, f"low-ST={low_to_st:.2f} <= {settings_target} (near ST support)"

        return False, ""

    def should_force_exit(self, client, opt, strategy_data):
        """Exit if 5-min candle close is below supertrend."""
        prev_candle, st = self._fetch_prev_candle_and_st(client, opt, strategy_data)
        if prev_candle is None:
            return False, ""

        if prev_candle["close"] < st:
            logger.info(
                f"[ST-DIP-{opt}] Close {prev_candle['close']:.2f} < ST {st:.2f} — force exit"
            )
            return True, f"close {prev_candle['close']:.2f} < supertrend {st:.2f}"

        return False, ""

    def on_target_hit(self, ctx: StrategyContext, strategy_data: dict,
                      lots_remaining: int) -> dict:
        return {"action": "trail"}

    # ── Backtest Support ─────────────────────────────────────────────

    def precompute_signal_data(self, candles, warmup_candles=None):
        """Pre-compute supertrend for backtest using 5-min bars.
        warmup_candles = previous day's 1-min candles for supertrend warmup."""
        if warmup_candles:
            all_candles = warmup_candles + candles
            warmup_len = len(warmup_candles)
        else:
            all_candles = candles
            warmup_len = 0

        # Aggregate 1-min candles into 5-min bars
        bars_5m = _aggregate_candles(all_candles, 5)
        st_vals, st_dirs = _compute_supertrend(bars_5m, ST_PERIOD, ST_MULTIPLIER)

        # Map each 1-min candle to its 5-min bar index
        all_per_minute_bar = [0] * len(all_candles)
        for bar_idx, bar in enumerate(bars_5m):
            for mi in bar["_minute_indices"]:
                all_per_minute_bar[mi] = bar_idx

        per_minute_bar = [all_per_minute_bar[warmup_len + i] for i in range(len(candles))]

        # Find first bar index that belongs to today's candles (not warmup)
        first_today_bar = per_minute_bar[0] if per_minute_bar else 0

        return {
            "bars_5m": bars_5m,
            "supertrend": st_vals,
            "direction": st_dirs,
            "per_minute_bar": per_minute_bar,
            "first_today_bar": first_today_bar,
            "_prev_bar_idx": -1,
            "_rolling_high": 0.0,
            "_entry_price": 0.0,
            "_target_points": None,
            "_sl_points": None,
        }

    def check_entry_signal_backtest(self, candles, signal_data, index, waiting):
        """Check entry at each 5-min bar close.
        Conditions checked on the CURRENT completed bar:
          1. rolling_high - bar_low > sl_points
          2. bar_close - supertrend <= target_points
          3. bar_close > supertrend
        Entry at current bar's close price."""
        bar_idx = signal_data["per_minute_bar"][index]

        # Only check at bar close (last 1-min candle of this 5-min bar)
        is_bar_close = (index == len(candles) - 1) or \
            (signal_data["per_minute_bar"][index + 1] != bar_idx)
        if not is_bar_close:
            return False

        if bar_idx == signal_data["_prev_bar_idx"]:
            return False
        signal_data["_prev_bar_idx"] = bar_idx

        # Update rolling high — only from bullish supertrend candles
        # Reset if supertrend turns negative
        first_today = signal_data["first_today_bar"]
        bar = signal_data["bars_5m"][bar_idx]
        st_dir = signal_data["direction"][bar_idx]

        if bar_idx >= first_today:
            if st_dir == 1:  # bullish
                signal_data["_rolling_high"] = max(signal_data["_rolling_high"], bar["high"])
            else:
                # Supertrend turned negative — reset rolling high
                signal_data["_rolling_high"] = 0.0

        st = signal_data["supertrend"][bar_idx]
        if st is None:
            return False

        target_pts = signal_data.get("_target_points")
        sl_pts = signal_data.get("_sl_points")
        if target_pts is None or sl_pts is None:
            return False

        rolling_high = signal_data["_rolling_high"]
        candle_low = bar["low"]
        close = bar["close"]
        dip_from_high = rolling_high - candle_low
        low_to_st = candle_low - st

        close_to_st = close - st

        # All four conditions
        if dip_from_high > sl_pts and low_to_st <= target_pts and close > st and close_to_st < target_pts:
            signal_data["_entry_price"] = candles[index]["close"]
            # Reset rolling high after entry
            signal_data["_rolling_high"] = candles[index]["close"]
            return True

        return False


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
