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
    def initial_direction(self, strategy_data: dict, bias: str) -> str:
        """Return the direction ("BUY" or "SELL") for the first trade of the day.

        *bias* is "BULLISH" or "BEARISH" (resolved from setting or market).
        The strategy should also set up strategy_data (e.g. option_type)
        based on the bias.
        """

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

    @property
    def signal_based(self) -> bool:
        """If True, engine uses dual-position signal-based mode."""
        return False

    def get_entry_signal(self, client, settings, strategy_data) -> list | None:
        """Return list of entry signals or None.

        Each signal dict: {"direction": "BUY", "option_type": "CE"/"PE"}.
        Only called when signal_based is True.
        """
        return None

    def on_target_hit(self, ctx: StrategyContext, strategy_data: dict,
                      lots_remaining: int) -> dict:
        """Called when a target level is hit. Default: just trail SL.

        Return:
            {"action": "trail"} — trail SL only (default)
            {"action": "partial_exit", "exit_lots": N} — exit N lots, then trail
        """
        return {"action": "trail"}

    def precompute_signal_data(self, candles, warmup_candles=None):
        """Pre-compute signal data for backtest. Override in signal-based strategies.

        warmup_candles: optional previous-day minute candles for indicator warmup.
        """
        return None

    def check_entry_signal_backtest(self, candles, signal_data, index, waiting):
        """Check entry signal at candle index for backtest. Override in signal-based strategies."""
        return False


# ─── Shared Helper ─────────────────────────────────────────────────────────


def _pick_monthly_expiry(expiries, ref_date):
    """Pick the nearest monthly expiry (last Tuesday of the month).

    Monthly expiry = the last expiry date within a calendar month.
    From sorted expiries, group by (year, month), pick the max in each group,
    then return the first one >= ref_date.
    """
    from collections import defaultdict
    by_month = defaultdict(list)
    for exp in expiries:
        by_month[(exp.year, exp.month)].append(exp)
    monthly = sorted(max(dates) for dates in by_month.values())
    for m in monthly:
        if m >= ref_date:
            return m
    return monthly[-1] if monthly else None


def select_nifty_option(client, option_type: str, min_premium: float = 0,
                        expiry_type: str = "weekly") -> dict | None:
    """Pick ATM NIFTY option — nearest strike to spot with premium >= min_premium.

    expiry_type: "weekly" (nearest expiry) or "monthly" (last expiry of the month).
    """
    try:
        instruments = client.kite.instruments("NFO")
        today = date.today()

        # ── Get NIFTY spot price ──
        try:
            spot_data = client.kite.ltp("NSE:NIFTY 50")
            spot = spot_data["NSE:NIFTY 50"]["last_price"]
        except Exception as e:
            logger.error(f"Failed to get NIFTY spot price: {e}")
            return None

        # ATM strike = nearest round-50 to spot (considers 25500, 25550, etc.)
        atm_strike = round(spot / 50) * 50
        logger.info(f"NIFTY spot: {spot:.1f}, ATM strike: {atm_strike}")

        # ── All NIFTY options of the requested type that haven't expired ──
        all_opts = [
            i for i in instruments
            if i["name"] == "NIFTY"
            and i["instrument_type"] == option_type
            and i["expiry"] >= today
        ]
        if not all_opts:
            logger.error(f"No NIFTY {option_type} options found")
            return None

        # ── Select expiry ──
        all_expiries = sorted({i["expiry"] for i in all_opts})
        if expiry_type == "monthly":
            target_expiry = _pick_monthly_expiry(all_expiries, today)
        else:
            target_expiry = all_expiries[0]  # nearest expiry

        if target_expiry is None:
            logger.error(f"No valid {expiry_type} expiry found")
            return None

        logger.info(
            f"{expiry_type.title()} expiry: {target_expiry} "
            f"({(target_expiry - today).days}d away)"
        )

        # ── Find candidates for this expiry, sorted by distance from ATM ──
        candidates = sorted(
            [i for i in all_opts if i["expiry"] == target_expiry],
            key=lambda i: abs(i["strike"] - atm_strike),
        )

        if not candidates:
            logger.error(f"No {option_type} strikes for {target_expiry}")
            return None

        # ── If no min_premium filter, just pick nearest ATM ──
        if min_premium <= 0:
            best = candidates[0]
            sym = f"NFO:{best['tradingsymbol']}"
            try:
                quote = client.kite.ltp(sym)
                premium = quote[sym]["last_price"]
            except Exception:
                premium = 0.0
            logger.info(
                f"Selected: {best['tradingsymbol']} "
                f"(strike: {best['strike']}, premium: {premium:.1f}, "
                f"expiry: {best['expiry']}, lot: {best.get('lot_size', 1)})"
            )
            return best

        # ── Sort nearest-to-ATM, ITM priority at same distance ──
        # CE ITM = lower strikes; PE ITM = higher strikes.
        if option_type == "CE":
            candidates.sort(key=lambda c: (
                0 if c["strike"] <= atm_strike else 1,
                abs(c["strike"] - atm_strike),
            ))
        else:
            candidates.sort(key=lambda c: (
                0 if c["strike"] >= atm_strike else 1,
                abs(c["strike"] - atm_strike),
            ))

        # ── Batch-fetch quotes for nearest 20 strikes ──
        check = candidates[:20]
        sym_map = {f"NFO:{c['tradingsymbol']}": c for c in check}
        try:
            quotes = client.kite.quote(list(sym_map.keys()))
        except Exception as e:
            logger.error(f"Batch quote fetch failed: {e}")
            quotes = {}

        # ── Find candidates in ±10% premium range, pick highest volume ──
        low = min_premium * 0.9
        high = min_premium * 1.1
        best_in_range = None
        best_volume = -1

        for c in check:
            sym = f"NFO:{c['tradingsymbol']}"
            q = quotes.get(sym, {})
            premium = q.get("last_price", 0)
            volume = q.get("volume", 0)
            oi = q.get("oi", 0)
            if low <= premium <= high and volume > best_volume:
                best_in_range = c
                best_volume = volume
                best_in_range["_premium"] = premium
                best_in_range["_volume"] = volume
                best_in_range["_oi"] = oi

        if best_in_range:
            logger.info(
                f"Selected: {best_in_range['tradingsymbol']} "
                f"(strike: {best_in_range['strike']}, premium: {best_in_range['_premium']:.1f}, "
                f"volume: {best_in_range.get('_volume', 0)}, OI: {best_in_range.get('_oi', 0)}, "
                f"min_premium: {min_premium}, "
                f"expiry: {best_in_range['expiry']}, lot: {best_in_range.get('lot_size', 1)})"
            )
            return best_in_range

        # Fallback: first option with premium >= min_premium
        for c in check:
            sym = f"NFO:{c['tradingsymbol']}"
            premium = quotes.get(sym, {}).get("last_price", 0)
            if premium >= min_premium:
                logger.warning(
                    f"No strike in {low:.0f}-{high:.0f} range with volume, "
                    f"falling back to {c['tradingsymbol']} (premium: {premium:.1f})"
                )
                return c

        # Last fallback: nearest ATM
        best = candidates[0]
        fallback_sym = f"NFO:{best['tradingsymbol']}"
        fallback_premium = quotes.get(fallback_sym, {}).get("last_price", 0)
        logger.warning(
            f"No strike with premium >= {min_premium}, "
            f"falling back to {best['tradingsymbol']} (premium: {fallback_premium:.1f})"
        )
        return best

    except Exception as e:
        logger.error(f"Instrument selection failed: {e}")
        return None
