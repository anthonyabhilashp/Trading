"""Buy Dip (Candle SL) — tracks rolling highs for both CE and PE options.
Enters at 5-min candle close when option drops target_points from rolling high.
Exits at 5-min candle close when (high_since_entry - close) > sl_points.
Can hold CE and PE simultaneously. Re-enters on next dip from rolling high."""

import logging
from datetime import datetime

import pytz

from ..base_strategy import (
    BaseStrategy,
    StrategyContext,
    register_strategy,
    select_nifty_option,
)

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")


@register_strategy("buy_dip_candle_close")
class BuyDipCandleStrategy(BaseStrategy):

    use_exchange_sl = False
    use_targets = False
    candle_sl_minutes = 5
    candle_sl_use_prev_low = True
    candle_check_offset = 1  # check at :01, :06, :11... (5-min candles arrive late)

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
        # Check at 1 minute past the 5-min boundary (5-min candles arrive late)
        now = datetime.now(IST)
        if now.minute % 5 != 1:
            return None
        check_key = f"{now.hour}:{now.minute}"
        if strategy_data.get("_entry_check") == check_key:
            return None
        strategy_data["_entry_check"] = check_key

        signals = []

        for opt in ("CE", "PE"):
            ltp = strategy_data.get(f"_ltp_{opt}", 0.0)
            if ltp <= 0:
                continue

            # Skip if already in a position for this option
            if strategy_data.get(f"_active_{opt}"):
                continue

            # Skip if this option is disabled for the day (lost money on it)
            if strategy_data.get(f"_disabled_{opt}"):
                continue

            # Fetch 5-min candles (checked at +1 min offset so they're available)
            token = strategy_data.get(f"_{opt.lower()}_token")
            if not token:
                continue

            try:
                today = now.date()
                candles = client.kite.historical_data(
                    token, today, today, "5minute"
                )
            except Exception as e:
                logger.error(f"[DIP-{opt}] Failed to fetch 5-min candles: {e}")
                continue

            if len(candles) < 2:
                continue

            # Update rolling high from all 5-min candle highs
            high_key = f"_rolling_high_{opt}"
            rolling_high = strategy_data.get(high_key, 0.0)
            for c in candles:
                if c["high"] > rolling_high:
                    rolling_high = c["high"]
            strategy_data[high_key] = rolling_high

            prev_bar_low = candles[-2]["low"]

            drop = rolling_high - prev_bar_low

            # Entry: previous candle's low dropped >= target_points from rolling high
            if drop >= settings.target_points:
                logger.info(
                    f"[DIP-{opt}] Entry signal: rolling_high={rolling_high:.2f}, "
                    f"prev_low={prev_bar_low:.2f}, drop={drop:.2f} >= target={settings.target_points}, "
                    f"entering at LTP={ltp:.2f}"
                )
                signals.append({"direction": "BUY", "option_type": opt})
                # Store entry LTP to detect losing trades on exit
                strategy_data[f"_entry_ltp_{opt}"] = ltp
                # Reset rolling high to current LTP for next re-entry tracking
                strategy_data[high_key] = ltp

        return signals if signals else None

    def on_sl_hit(self, ctx: StrategyContext, strategy_data: dict) -> dict:
        opt = strategy_data.get("_sl_hit_option", "CE")
        strategy_data[f"_waiting_{opt}"] = True
        strategy_data[f"_active_{opt}"] = False

        # If exited below entry price, disable this option for the day
        entry_ltp = strategy_data.get(f"_entry_ltp_{opt}", 0)
        current_ltp = strategy_data.get(f"_ltp_{opt}", 0)
        if entry_ltp > 0 and current_ltp < entry_ltp:
            strategy_data[f"_disabled_{opt}"] = True
            logger.info(
                f"[DIP-{opt}] Losing trade (entry={entry_ltp:.2f}, exit~={current_ltp:.2f}) "
                f"— disabled for the day"
            )
        else:
            logger.info(f"[DIP-{opt}] SL hit with profit — waiting for next dip signal")

        return {"action": "stop"}

    def on_target_hit(self, ctx: StrategyContext, strategy_data: dict,
                      lots_remaining: int) -> dict:
        return {"action": "trail"}

    # ── Backtest Support ─────────────────────────────────────────────

    def precompute_signal_data(self, candles, warmup_candles=None):
        """Pre-compute rolling high data for backtest.
        Tracks rolling high across candles to detect dip entries."""
        return {
            "_rolling_high": 0.0,
            "_target_points": None,  # set from settings during simulation
            "_disabled": False,      # set True after a losing trade
            "_entry_price": 0.0,     # track entry price for loss detection
        }

    def check_entry_signal_backtest(self, candles, signal_data, index, waiting):
        """Check if previous candle's low dropped target_points from rolling high."""
        # Skip if disabled after a losing trade
        if signal_data.get("_disabled"):
            return False

        candle = candles[index]
        high = candle["high"]

        # Update rolling high from current candle
        if high > signal_data["_rolling_high"]:
            signal_data["_rolling_high"] = high

        # Need at least one previous candle
        if index < 1:
            return False

        rolling_high = signal_data["_rolling_high"]

        target_pts = signal_data.get("_target_points")
        if target_pts is None:
            return False

        # Check previous candle's low for the dip
        prev_low = candles[index - 1]["low"]
        drop = rolling_high - prev_low

        if drop >= target_pts:
            # Enter at current candle's close
            close = candle["close"]
            signal_data["_entry_price"] = close
            # Reset rolling high after entry
            signal_data["_rolling_high"] = close
            return True

        return False
