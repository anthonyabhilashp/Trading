"""Base strategy class, registry, and shared instrument selection helper."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

import logging

logger = logging.getLogger(__name__)

# ─── Strategy Registry ─────────────────────────────────────────────────────

STRATEGY_REGISTRY: dict[str, type["BaseStrategy"]] = {}


def register_strategy(name: str):
    """Class decorator that registers a strategy under *name*."""
    def decorator(cls):
        STRATEGY_REGISTRY[name] = cls
        cls.registry_name = name
        return cls
    return decorator


# ─── Context passed to strategy methods ────────────────────────────────────

@dataclass(frozen=True)
class StrategyContext:
    """Read-only snapshot the engine passes to strategy decision methods."""
    client: Any           # KiteClient
    settings: Any         # StrategySettings
    trading_symbol: str
    current_direction: str  # direction of the position that just got stopped out


# ─── Base Strategy ─────────────────────────────────────────────────────────

class BaseStrategy(ABC):
    """All strategies implement these three decision points."""

    @abstractmethod
    def select_instrument(self, ctx: StrategyContext, strategy_data: dict) -> dict | None:
        """Pick trading instrument. Return the instrument dict or None.

        The implementation should call ``select_nifty_option`` (or similar)
        and may read/write *strategy_data* for persistent state like
        which option_type to use.  The engine handles all state mutation.
        """

    @abstractmethod
    def initial_direction(self, strategy_data: dict) -> str:
        """Return the direction ("BUY" or "SELL") for the first trade of the day."""

    @abstractmethod
    def on_sl_hit(self, ctx: StrategyContext, strategy_data: dict) -> dict:
        """Decide what to do when the stop-loss is hit.

        Return an action dict, one of:
            {"action": "reverse", "direction": "<new_dir>"}
            {"action": "reselect_and_enter", "direction": "<new_dir>"}
            {"action": "stop"}
        """

    @property
    def lot_multiplier(self) -> int:
        """Minimum number of lots per entry. Default 1."""
        return 1

    def on_target_hit(self, ctx: StrategyContext, strategy_data: dict,
                      lots_remaining: int) -> dict:
        """Called when a target level is hit. Default: just trail SL.

        Return:
            {"action": "trail"} — trail SL only (default)
            {"action": "partial_exit", "exit_lots": N} — exit N lots, then trail
        """
        return {"action": "trail"}


# ─── Shared Helper ─────────────────────────────────────────────────────────

def select_nifty_option(client, settings, option_type: str) -> dict | None:
    """Pick NIFTY monthly option with premium closest to target.

    Returns the chosen instrument dict on success, or None on failure.
    This is shared across strategies to avoid duplicating instrument
    selection logic.
    """
    try:
        instruments = client.kite.instruments("NFO")
        today = date.today()
        min_expiry = today + timedelta(days=30)

        all_opts = [
            i for i in instruments
            if i["name"] == "NIFTY"
            and i["instrument_type"] == option_type
            and i["expiry"] >= min_expiry
        ]
        if not all_opts:
            logger.error(f"No NIFTY {option_type} with expiry >= {min_expiry}")
            return None

        # Identify monthly expiries (latest expiry per calendar month)
        all_expiries = {
            i["expiry"] for i in instruments
            if i["name"] == "NIFTY"
            and i["instrument_type"] == option_type
        }
        month_max: dict[tuple[int, int], date] = {}
        for exp in all_expiries:
            key = (exp.year, exp.month)
            if key not in month_max or exp > month_max[key]:
                month_max[key] = exp
        monthly_expiries = set(month_max.values())

        # Filter to monthly, pick the nearest monthly expiry
        monthly_opts = [i for i in all_opts if i["expiry"] in monthly_expiries]
        if not monthly_opts:
            logger.warning("No monthly expiry found, falling back to all")
            monthly_opts = all_opts

        target_expiry = min(i["expiry"] for i in monthly_opts)
        candidates = [i for i in monthly_opts if i["expiry"] == target_expiry]
        logger.info(
            f"Monthly expiry: {target_expiry}, "
            f"{len(candidates)} {option_type} strikes available"
        )

        # Fetch LTP for all strikes of this expiry to find target premium
        sym_map: dict[str, dict] = {}
        for c in candidates:
            sym_map[f"NFO:{c['tradingsymbol']}"] = c

        ltp_data = client.kite.ltp(list(sym_map.keys()))

        target = settings.target_premium
        best = None
        best_diff = float("inf")
        best_premium = 0.0
        for sym, data in ltp_data.items():
            premium = data["last_price"]
            if premium <= 0:
                continue
            diff = abs(premium - target)
            if diff < best_diff:
                best_diff = diff
                best = sym_map[sym]
                best_premium = premium

        if not best:
            logger.error("No strike with valid LTP found")
            return None

        if best_diff > target * 0.2:
            logger.warning(
                f"Closest premium {best_premium:.1f} is far from "
                f"target {target:.0f}, proceeding anyway"
            )

        logger.info(
            f"Selected: {best['tradingsymbol']} "
            f"(strike: {best['strike']}, premium: {best_premium:.1f}, "
            f"expiry: {best['expiry']}, lot: {best.get('lot_size', 1)})"
        )
        return best

    except Exception as e:
        logger.error(f"Instrument selection failed: {e}")
        return None
