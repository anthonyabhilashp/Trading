"""Backtest simulation engine — runs a strategy on historical 1-min option candles."""

import logging
from datetime import datetime

import pytz

from .base_strategy import STRATEGY_REGISTRY, StrategyContext, select_nifty_option
from .strategy import StrategySettings

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")
MIN_PREMIUM = 20.0
NIFTY_INDEX_TOKEN = 256265  # NSE:NIFTY 50 instrument token


class BacktestEngine:
    """Simulates a strategy on historical 1-min candles for a single day."""

    def __init__(self, client):
        self.client = client
        self._candle_cache: dict[int, list] = {}  # token → candles

    # ── Public API ─────────────────────────────────────────────────────────

    def run(self, config: dict) -> dict:
        date_str = config["date"]
        strategy_name = config["strategy_name"]
        sl_points = float(config["sl_points"])
        target_points = float(config["target_points"])
        quantity = int(config["quantity"])
        start_time = config["start_time"]
        stop_time = config["stop_time"]
        market_bias = config.get("market_bias", "BULLISH").upper()
        min_premium = float(config.get("min_premium", 100))
        expiry_type = config.get("expiry_type", "weekly")
        daily_cutoff = bool(config.get("daily_cutoff", False))
        daily_profit_pct = float(config.get("daily_profit_pct", 0))
        daily_loss_pct = float(config.get("daily_loss_pct", 0))
        cutoff = {"enabled": daily_cutoff, "profit_pct": daily_profit_pct, "loss_pct": daily_loss_pct}

        if strategy_name not in STRATEGY_REGISTRY:
            return {"error": f"Unknown strategy: {strategy_name}"}
        if sl_points <= 0 or target_points <= 0:
            return {"error": "sl_points and target_points must be positive"}

        strategy = STRATEGY_REGISTRY[strategy_name]()

        # Enforce lot multiplier
        mult = strategy.lot_multiplier
        if quantity < mult or quantity % mult != 0:
            quantity = max(1, round(quantity / mult)) * mult
            logger.info(f"Backtest: lots auto-corrected to {quantity} (multiplier={mult})")

        instruments = config.get("_instruments") or self.client.kite.instruments("NFO")
        self._candle_cache = {} if not config.get("_shared_cache") else config["_shared_cache"]

        settings = StrategySettings(
            sl_points=sl_points,
            target_points=target_points,
            quantity=quantity,
            start_time=start_time,
            stop_time=stop_time,
            market_bias=market_bias,
            min_premium=min_premium,
            expiry_type=expiry_type,
        )

        # ── Signal-based mode (dual CE + PE) ──
        if strategy.signal_based:
            return self._run_signal_based(
                strategy, settings, instruments, date_str,
                min_premium, start_time, stop_time, expiry_type,
                cutoff=cutoff,
            )

        # ── Single-position mode ──
        # Let the strategy decide option_type via initial_direction
        strategy_data = {}
        strategy.initial_direction(strategy_data, market_bias)
        # SAR doesn't set option_type (always CE); buy_alternate/buy_scale_out set it
        option_type = strategy_data.get("option_type", "CE")

        # Pre-fetch NIFTY index minute candles for mid-day spot lookups
        nifty_candles = self._fetch_nifty_minute_candles(date_str)

        instrument, candles, err = self._auto_select_instrument(
            instruments, option_type, date_str, min_premium,
            start_time, stop_time, expiry_type=expiry_type,
        )
        if err:
            return {"error": err}

        # Resolve percentage cutoff into ₹ using actual lot_size
        lot_size = int(instrument.get("lot_size", 1))
        notional = quantity * lot_size * min_premium
        cutoff = self._resolve_cutoff(cutoff, notional)

        if getattr(strategy, 'candle_sl_minutes', 0) > 0:
            if strategy.lot_multiplier > 1:
                trades = self._simulate_candle_scaleout(
                    candles, strategy, strategy_data, settings,
                    instrument, instruments, date_str, nifty_candles,
                    cutoff=cutoff,
                )
            else:
                trades = self._simulate_candle_alt(
                    candles, strategy, strategy_data, settings,
                    instrument, instruments, date_str, nifty_candles,
                    cutoff=cutoff,
                )
        else:
            trades = self._simulate(
                candles, strategy, strategy_data, settings,
                instrument, instruments, date_str, nifty_candles,
                cutoff=cutoff,
            )

        total_pnl = sum(t["pnl"] for t in trades)
        wins = sum(1 for t in trades if t["pnl"] > 0)
        losses = sum(1 for t in trades if t["pnl"] < 0)

        # Collect all unique instruments from trades
        instruments_used = list(dict.fromkeys(
            t["symbol"] for t in trades if t.get("symbol")
        ))

        return {
            "trades": trades,
            "summary": {
                "total_pnl": round(total_pnl, 2),
                "wins": wins,
                "losses": losses,
                "total_trades": len(trades),
                "instrument": instrument["tradingsymbol"],
                "strike": instrument["strike"],
                "instruments_used": instruments_used,
            },
        }

    # ── Instrument / Candle Helpers ────────────────────────────────────────

    def _get_historical_spot(self, date_str) -> float | None:
        """Fetch NIFTY 50 index open price for the given date."""
        target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        try:
            candles = self.client.kite.historical_data(
                NIFTY_INDEX_TOKEN, target_date, target_date, "day",
            )
            if candles:
                return candles[0]["open"]
        except Exception as e:
            logger.error(f"Failed to fetch NIFTY spot for {date_str}: {e}")
        return None

    def _fetch_nifty_minute_candles(self, date_str) -> list:
        """Fetch NIFTY 50 index 1-min candles for the given date."""
        target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        try:
            return self.client.kite.historical_data(
                NIFTY_INDEX_TOKEN, target_date, target_date, "minute",
            )
        except Exception as e:
            logger.error(f"Failed to fetch NIFTY minute candles for {date_str}: {e}")
            return []

    def _get_spot_at_time(self, nifty_candles, check_time) -> float | None:
        """Get NIFTY spot (close) at a specific time from minute candles."""
        best = None
        for c in nifty_candles:
            cdt = c["date"]
            if cdt.tzinfo is None:
                cdt = IST.localize(cdt)
            if cdt <= check_time:
                best = c["close"]
            else:
                break
        return best

    def _auto_select_instrument(self, instruments, option_type, date_str,
                                min_premium, start_time, stop_time,
                                check_time=None, spot_override=None,
                                expiry_type="weekly"):
        """Auto-select nearest-to-ATM strike with premium >= min_premium.

        Mirrors the live engine's select_nifty_option: searches strikes sorted
        by distance from ATM (both ITM and OTM), picks the first with premium
        >= min_premium.

        Args:
            check_time:    Optional aware datetime; when given, check premium at
                           this candle time instead of market open.
            spot_override:  Optional NIFTY spot at reselection time; if given,
                           ATM is recomputed from this spot.
            expiry_type:   "weekly" (nearest expiry) or "monthly" (last of month).

        Returns (instrument, filtered_candles, error_string).
        """
        target_date = datetime.strptime(date_str, "%Y-%m-%d").date()

        # Use live spot at reselection time, or day open for initial
        spot = spot_override or self._get_historical_spot(date_str)
        if spot is None:
            return None, None, f"Could not fetch NIFTY spot for {date_str} (holiday?)"

        atm_strike = round(spot / 50) * 50
        logger.info(
            f"Backtest: NIFTY spot {spot:.1f}, ATM strike {atm_strike}"
            f"{' (reselect)' if spot_override else ''}"
        )

        # All NIFTY options of this type with expiry >= date
        all_opts = [
            i for i in instruments
            if i["name"] == "NIFTY"
            and i["instrument_type"] == option_type
            and i["expiry"] >= target_date
        ]
        if not all_opts:
            return None, None, f"No NIFTY {option_type} instruments found"

        # Select expiry based on type
        if expiry_type == "monthly":
            from .base_strategy import _pick_monthly_expiry
            all_expiries = sorted({i["expiry"] for i in all_opts})
            target_expiry = _pick_monthly_expiry(all_expiries, target_date)
            if target_expiry is None:
                return None, None, f"No monthly expiry found for {option_type}"
        else:
            target_expiry = min(i["expiry"] for i in all_opts)
        candidates = [i for i in all_opts if i["expiry"] == target_expiry]

        # When min_premium is set, search ITM-first (higher premiums) then OTM.
        # CE ITM = lower strikes; PE ITM = higher strikes.
        if min_premium > 0:
            if option_type == "CE":
                # Lower strikes first (ITM), then higher (OTM)
                candidates.sort(key=lambda c: (
                    0 if c["strike"] <= atm_strike else 1,  # ITM first
                    abs(c["strike"] - atm_strike),           # nearest to ATM within group
                ))
            else:
                # Higher strikes first (ITM for PE), then lower (OTM)
                candidates.sort(key=lambda c: (
                    0 if c["strike"] >= atm_strike else 1,
                    abs(c["strike"] - atm_strike),
                ))
        else:
            candidates.sort(key=lambda c: abs(c["strike"] - atm_strike))

        fallback_inst = None
        fallback_filtered = None
        fallback_premium = 0.0

        # Collect all valid candidates with premium and volume info
        # Then pick the best match (mirrors live: ±10% premium range, highest volume)
        in_range = []  # [(inst, filtered, premium, volume)]
        above_min = []  # [(inst, filtered, premium, volume)]

        low = min_premium * 0.9
        high = min_premium * 1.1

        past_range = 0  # count candidates checked whose premium is outside range

        for inst in candidates[:40]:
            # Use 15-min candles for both premium and volume (single fetch)
            candles_15m = self._fetch_15m_candles(inst, date_str)
            if not candles_15m:
                continue

            if check_time is not None:
                # Find 15-min candle at or just before check_time
                check_candle = None
                for c in candles_15m:
                    c_dt = c["date"]
                    if c_dt.tzinfo is None:
                        c_dt = IST.localize(c_dt)
                    if c_dt >= check_time:
                        break
                    check_candle = c
                if check_candle is None:
                    check_candle = candles_15m[0]
                check_price = check_candle["close"]
                check_vol = check_candle.get("volume", 0)
            else:
                check_price = candles_15m[0]["open"]
                check_vol = candles_15m[0].get("volume", 0)

            # Track nearest-to-ATM as fallback (first valid candidate)
            if fallback_inst is None:
                fallback_inst = inst
                fallback_premium = check_price

            if min_premium <= 0:
                # Fetch 1-min candles only for the selected instrument
                sel_candles = self._fetch_candles(inst, date_str)
                sel_filtered = self._filter_candles(sel_candles, date_str, start_time, stop_time) if sel_candles else None
                return inst, sel_filtered, None

            if low <= check_price <= high:
                in_range.append((inst, check_price, check_vol))
            elif check_price >= min_premium:
                above_min.append((inst, check_price, check_vol))
                past_range += 1
            else:
                past_range += 1

            # Stop early: if we have candidates in range and checked enough beyond
            if in_range and past_range >= 3:
                break

        # Pick highest volume in ±10% premium range (matches live logic)
        if in_range:
            in_range.sort(key=lambda x: x[2], reverse=True)
            best_inst, best_prem, best_vol = in_range[0]
            candles = self._fetch_candles(best_inst, date_str)
            filtered = self._filter_candles(candles, date_str, start_time, stop_time) if candles else None
            logger.info(
                f"Backtest: selected {best_inst['tradingsymbol']} "
                f"(strike {best_inst['strike']}, premium {best_prem:.1f}, "
                f"volume {best_vol}, best of {len(in_range)} in range)"
            )
            return best_inst, filtered, None

        # Fallback: first option with premium >= min_premium
        if above_min:
            best_inst, best_prem, best_vol = above_min[0]
            candles = self._fetch_candles(best_inst, date_str)
            filtered = self._filter_candles(candles, date_str, start_time, stop_time) if candles else None
            logger.warning(
                f"Backtest: no strike in {low:.0f}-{high:.0f} range with volume, "
                f"falling back to {best_inst['tradingsymbol']} (premium {best_prem:.1f})"
            )
            return best_inst, filtered, None

        # Last fallback: nearest ATM even if premium < min_premium
        if fallback_inst is not None:
            candles = self._fetch_candles(fallback_inst, date_str)
            filtered = self._filter_candles(candles, date_str, start_time, stop_time) if candles else None
            logger.warning(
                f"Backtest: no {option_type} with premium >= {min_premium}, "
                f"falling back to {fallback_inst['tradingsymbol']} "
                f"(premium {fallback_premium:.1f})"
            )
            return fallback_inst, filtered, None

        return None, None, (
            f"No {option_type} strike near ATM {atm_strike} on {date_str}"
        )

    def _find_instrument(self, instruments, strike, option_type, date_str,
                         target_expiry=None):
        target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        candidates = [
            i for i in instruments
            if i["name"] == "NIFTY"
            and i["instrument_type"] == option_type
            and i["strike"] == strike
        ]
        if target_expiry:
            exact = [i for i in candidates if i["expiry"] == target_expiry]
            if exact:
                return exact[0]
        valid = [i for i in candidates if i["expiry"] >= target_date]
        if not valid:
            return None
        return min(valid, key=lambda i: i["expiry"])

    def _fetch_candles(self, instrument, date_str):
        token = instrument["instrument_token"]
        if token in self._candle_cache:
            return self._candle_cache[token]
        target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        candles = self.client.kite.historical_data(
            token, target_date, target_date, "minute",
        )
        self._candle_cache[token] = candles
        return candles

    def _fetch_15m_candles(self, instrument, date_str):
        """Fetch 15-minute candles for instrument selection (premium + volume)."""
        token = instrument["instrument_token"]
        target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        cache_key = (token, "15m")
        if cache_key in self._candle_cache:
            return self._candle_cache[cache_key]
        candles_15m = self.client.kite.historical_data(
            token, target_date, target_date, "15minute",
        )
        self._candle_cache[cache_key] = candles_15m
        return candles_15m

    def _fetch_prev_day_candles(self, instrument, date_str):
        """Fetch previous trading day's minute candles for indicator warmup."""
        from datetime import timedelta
        target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        prev_date = target_date - timedelta(days=1)
        token = instrument["instrument_token"]
        cache_key = (token, "prev", prev_date.isoformat())
        if cache_key in self._candle_cache:
            return self._candle_cache[cache_key]
        try:
            candles = self.client.kite.historical_data(
                token, prev_date, prev_date, "minute",
            )
        except Exception:
            candles = []
        self._candle_cache[cache_key] = candles
        return candles

    def _filter_candles(self, candles, date_str, start_time, stop_time):
        target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        start_dt = IST.localize(datetime.combine(
            target_date, datetime.strptime(start_time, "%H:%M").time(),
        ))
        stop_dt = IST.localize(datetime.combine(
            target_date, datetime.strptime(stop_time, "%H:%M").time(),
        ))
        filtered = []
        for c in candles:
            cdt = c["date"]
            if cdt.tzinfo is None:
                cdt = IST.localize(cdt)
            if start_dt <= cdt <= stop_dt:
                filtered.append(c)
        return filtered

    # ── Daily Cutoff Helper ─────────────────────────────────────────────

    @staticmethod
    def _resolve_cutoff(cutoff, notional):
        """Convert percentage-based cutoff into ₹ values."""
        if not cutoff or not cutoff.get("enabled"):
            return {"enabled": False, "profit": 0, "loss": 0}
        profit_pct = cutoff.get("profit_pct", 0)
        loss_pct = cutoff.get("loss_pct", 0)
        return {
            "enabled": True,
            "profit": round(notional * profit_pct / 100) if profit_pct > 0 else 0,
            "loss": round(notional * loss_pct / 100) if loss_pct > 0 else 0,
        }

    @staticmethod
    def _cutoff_hit(trades, cutoff):
        """Check if daily P&L has hit profit target or loss limit."""
        if not cutoff or not cutoff["enabled"]:
            return False
        total = sum(t["pnl"] for t in trades)
        if cutoff["profit"] > 0 and total >= cutoff["profit"]:
            return True
        if cutoff["loss"] > 0 and total <= -cutoff["loss"]:
            return True
        return False

    # ── Simulation Core ────────────────────────────────────────────────────

    @staticmethod
    def _candle_time(candle) -> str:
        cdt = candle["date"]
        if hasattr(cdt, "strftime"):
            return cdt.strftime("%H:%M:%S")
        return str(cdt)

    @staticmethod
    def _make_position(direction, entry_price, settings, entry_time, quantity):
        sl = settings.sl_points
        tgt = settings.target_points
        if direction == "BUY":
            return {
                "direction": direction, "entry_price": entry_price,
                "sl_price": entry_price - sl, "target_price": entry_price + tgt,
                "entry_time": entry_time, "remaining_lots": quantity,
            }
        return {
            "direction": direction, "entry_price": entry_price,
            "sl_price": entry_price + sl, "target_price": entry_price - tgt,
            "entry_time": entry_time, "remaining_lots": quantity,
        }

    @staticmethod
    def _calc_pnl(direction, entry, exit_price, lots, lot_size):
        if direction == "BUY":
            return (exit_price - entry) * lots * lot_size
        return (entry - exit_price) * lots * lot_size

    def _record_trade(self, position, exit_price, time_str, symbol, lot_size,
                      lots, reason):
        pnl = self._calc_pnl(
            position["direction"], position["entry_price"],
            exit_price, lots, lot_size,
        )
        return {
            "direction": position["direction"],
            "symbol": symbol,
            "entry_price": round(position["entry_price"], 2),
            "exit_price": round(exit_price, 2),
            "entry_time": position["entry_time"],
            "exit_time": time_str,
            "lots": lots,
            "pnl": round(pnl, 2),
            "exit_reason": reason,
        }

    # ── Signal-Based Backtest ─────────────────────────────────────────────

    def _run_signal_based(self, strategy, settings, instruments, date_str,
                          min_premium, start_time, stop_time,
                          expiry_type="weekly", cutoff=None):
        """Run backtest for signal-based strategies (dual CE + PE positions)."""
        cutoff = cutoff or {"enabled": False, "profit": 0, "loss": 0}
        # Select both CE and PE instruments
        ce_inst, ce_candles, ce_err = self._auto_select_instrument(
            instruments, "CE", date_str, min_premium, start_time, stop_time,
            expiry_type=expiry_type,
        )
        pe_inst, pe_candles, pe_err = self._auto_select_instrument(
            instruments, "PE", date_str, min_premium, start_time, stop_time,
            expiry_type=expiry_type,
        )

        if ce_err and pe_err:
            return {"error": f"CE: {ce_err}; PE: {pe_err}"}

        # Resolve percentage cutoff into ₹ using actual lot_size
        primary_inst = ce_inst or pe_inst
        lot_size = int(primary_inst.get("lot_size", 1)) if primary_inst else 1
        notional = settings.quantity * lot_size * min_premium
        cutoff = self._resolve_cutoff(cutoff, notional)

        if getattr(strategy, 'candle_sl_minutes', 0) > 0:
            trades = self._simulate_signal_candle_sl(
                strategy, settings, ce_inst, ce_candles, pe_inst, pe_candles,
                date_str=date_str, cutoff=cutoff,
            )
        else:
            trades = self._simulate_signal_based(
                strategy, settings, ce_inst, ce_candles, pe_inst, pe_candles,
                date_str=date_str, cutoff=cutoff,
            )

        total_pnl = sum(t["pnl"] for t in trades)
        wins = sum(1 for t in trades if t["pnl"] > 0)
        losses = sum(1 for t in trades if t["pnl"] < 0)

        instruments_used = list(dict.fromkeys(
            t["symbol"] for t in trades if t.get("symbol")
        ))

        primary_inst = ce_inst or pe_inst

        return {
            "trades": trades,
            "summary": {
                "total_pnl": round(total_pnl, 2),
                "wins": wins,
                "losses": losses,
                "total_trades": len(trades),
                "instrument": primary_inst["tradingsymbol"] if primary_inst else "-",
                "strike": primary_inst["strike"] if primary_inst else 0,
                "instruments_used": instruments_used,
            },
        }

    def _simulate_signal_based(self, strategy, settings,
                               ce_inst, ce_candles, pe_inst, pe_candles,
                               date_str="", cutoff=None):
        """Simulate dual CE + PE positions using strategy signal methods."""
        trades = []
        strategy_data = {}
        entry_floor = MIN_PREMIUM

        # Build per-option state
        slots = {}
        for opt, inst, candles in [("CE", ce_inst, ce_candles), ("PE", pe_inst, pe_candles)]:
            if inst is None or candles is None:
                continue
            # Fetch previous day candles for indicator warmup
            warmup = self._fetch_prev_day_candles(inst, date_str) if date_str else []
            signal_data = strategy.precompute_signal_data(candles, warmup or None)
            slots[opt] = {
                "inst": inst,
                "candles": candles,
                "signal_data": signal_data,
                "symbol": inst["tradingsymbol"],
                "lot_size": int(inst.get("lot_size", 1)),
                "position": None,
                "waiting": False,  # waiting for next signal after SL
            }

        if not slots:
            return trades

        # Align candles by time — iterate over all unique timestamps
        all_times = set()
        for opt, s in slots.items():
            for i, c in enumerate(s["candles"]):
                cdt = c["date"]
                if cdt.tzinfo is None:
                    cdt = IST.localize(cdt)
                all_times.add(cdt)

        sorted_times = sorted(all_times)

        # Build index maps: time → candle index
        for opt, s in slots.items():
            time_map = {}
            for i, c in enumerate(s["candles"]):
                cdt = c["date"]
                if cdt.tzinfo is None:
                    cdt = IST.localize(cdt)
                time_map[cdt] = i
            s["time_map"] = time_map

        cutoff_reached = False
        for t_idx, cur_time in enumerate(sorted_times):
            is_last_time = (t_idx == len(sorted_times) - 1)

            for opt, s in slots.items():
                ci = s["time_map"].get(cur_time)
                if ci is None:
                    continue

                candle = s["candles"][ci]
                time_str = self._candle_time(candle)
                symbol = s["symbol"]
                lot_size = s["lot_size"]
                position = s["position"]

                if position:
                    # ── SL check ──
                    sl_hit = False
                    if (position["direction"] == "BUY"
                            and candle["low"] <= position["sl_price"]):
                        sl_hit = True

                    if sl_hit:
                        trades.append(self._record_trade(
                            position, position["sl_price"], time_str,
                            symbol, lot_size, position["remaining_lots"], "SL",
                        ))
                        s["position"] = None
                        s["waiting"] = True
                        if self._cutoff_hit(trades, cutoff):
                            cutoff_reached = True
                        continue

                    # ── Target check with trailing ──
                    targets_crossed = 0
                    exit_levels = []

                    if position["direction"] == "BUY":
                        while candle["high"] >= position["target_price"]:
                            exit_levels.append(position["target_price"])
                            position["target_price"] += settings.target_points
                            position["sl_price"] += settings.sl_points
                            targets_crossed += 1

                    for k in range(targets_crossed):
                        ctx = StrategyContext(
                            client=self.client, settings=settings,
                            trading_symbol=symbol,
                            current_direction=position["direction"],
                        )
                        result = strategy.on_target_hit(
                            ctx, strategy_data, position["remaining_lots"],
                        )
                        if (result.get("action") == "partial_exit"
                                and position["remaining_lots"] > 1):
                            exit_n = min(
                                result.get("exit_lots", 1),
                                position["remaining_lots"] - 1,
                            )
                            trades.append(self._record_trade(
                                position, exit_levels[k], time_str,
                                symbol, lot_size, exit_n, "Partial",
                            ))
                            position["remaining_lots"] -= exit_n

                    # ── Square off at last candle ──
                    if is_last_time and s["position"]:
                        trades.append(self._record_trade(
                            s["position"], candle["close"], time_str,
                            symbol, lot_size,
                            s["position"]["remaining_lots"], "EOD",
                        ))
                        s["position"] = None

                else:
                    # ── No position: check strategy signal for entry ──
                    if is_last_time or cutoff_reached:
                        continue

                    should_enter = strategy.check_entry_signal_backtest(
                        s["candles"], s["signal_data"], ci, s["waiting"],
                    )
                    if should_enter and s["waiting"]:
                        s["waiting"] = False

                    if should_enter:
                        close = candle["close"]
                        if close >= entry_floor:
                            s["position"] = self._make_position(
                                "BUY", close, settings, time_str,
                                settings.quantity,
                            )

        return trades

    # ── Signal-Based Candle-SL Simulation ─────────────────────────────────

    def _simulate_signal_candle_sl(self, strategy, settings,
                                   ce_inst, ce_candles, pe_inst, pe_candles,
                                   date_str="", cutoff=None):
        """Signal-based simulation with candle-close SL: exit at N-min close
        when (high_since_entry - close) > sl_points. No exchange SL, no trailing."""
        trades = []
        strategy_data = {}
        entry_floor = MIN_PREMIUM
        bar_min = getattr(strategy, 'candle_sl_minutes', 5)

        slots = {}
        for opt, inst, candles in [("CE", ce_inst, ce_candles), ("PE", pe_inst, pe_candles)]:
            if inst is None or candles is None:
                continue
            warmup = self._fetch_prev_day_candles(inst, date_str) if date_str else []
            signal_data = strategy.precompute_signal_data(candles, warmup or None)
            slots[opt] = {
                "inst": inst,
                "candles": candles,
                "signal_data": signal_data,
                "symbol": inst["tradingsymbol"],
                "lot_size": int(inst.get("lot_size", 1)),
                "position": None,
                "waiting": False,
                "high_since_entry": 0.0,
            }

        if not slots:
            return trades

        all_times = set()
        for opt, s in slots.items():
            for i, c in enumerate(s["candles"]):
                cdt = c["date"]
                if cdt.tzinfo is None:
                    cdt = IST.localize(cdt)
                all_times.add(cdt)

        sorted_times = sorted(all_times)

        for opt, s in slots.items():
            time_map = {}
            for i, c in enumerate(s["candles"]):
                cdt = c["date"]
                if cdt.tzinfo is None:
                    cdt = IST.localize(cdt)
                time_map[cdt] = i
            s["time_map"] = time_map

        cutoff_reached = False
        for t_idx, cur_time in enumerate(sorted_times):
            is_last_time = (t_idx == len(sorted_times) - 1)

            for opt, s in slots.items():
                ci = s["time_map"].get(cur_time)
                if ci is None:
                    continue

                candle = s["candles"][ci]
                time_str = self._candle_time(candle)
                symbol = s["symbol"]
                lot_size = s["lot_size"]
                position = s["position"]

                if position:
                    # Track high since entry
                    s["high_since_entry"] = max(s["high_since_entry"], candle["high"])

                    # Check at N-min bar close or last candle
                    cdt = candle["date"]
                    is_bar_close = (cdt.minute % bar_min == bar_min - 1) or is_last_time

                    if is_bar_close:
                        close = candle["close"]
                        drop = s["high_since_entry"] - close

                        if drop > settings.sl_points and not is_last_time:
                            # Candle SL hit — exit
                            trades.append(self._record_trade(
                                position, close, time_str,
                                symbol, lot_size, position["remaining_lots"], "SL",
                            ))
                            s["position"] = None
                            s["waiting"] = True
                            s["high_since_entry"] = 0.0
                            if self._cutoff_hit(trades, cutoff):
                                cutoff_reached = True
                            continue

                    # Square off at last candle
                    if is_last_time and s["position"]:
                        trades.append(self._record_trade(
                            s["position"], candle["close"], time_str,
                            symbol, lot_size,
                            s["position"]["remaining_lots"], "EOD",
                        ))
                        s["position"] = None

                else:
                    # No position: check strategy signal for entry
                    if is_last_time or cutoff_reached:
                        continue

                    should_enter = strategy.check_entry_signal_backtest(
                        s["candles"], s["signal_data"], ci, s["waiting"],
                    )
                    if should_enter and s["waiting"]:
                        s["waiting"] = False

                    if should_enter:
                        close = candle["close"]
                        if close >= entry_floor:
                            s["position"] = self._make_position(
                                "BUY", close, settings, time_str,
                                settings.quantity,
                            )
                            s["high_since_entry"] = close

        return trades

    # ── Single-Position Simulation ─────────────────────────────────────────

    def _simulate(self, candles, strategy, strategy_data, settings,
                  current_instrument, all_instruments, date_str,
                  nifty_candles=None, cutoff=None):
        trades = []
        position = None
        pending_direction = None
        symbol = current_instrument["tradingsymbol"]
        lot_size = int(current_instrument.get("lot_size", 1))
        # min_premium is for instrument selection, not entry gating.
        # Once instrument is selected, only enforce a basic floor.
        entry_floor = MIN_PREMIUM

        # Get the initial direction from the strategy
        pending_direction = strategy.initial_direction(
            strategy_data, settings.market_bias,
        )

        i = 0
        while i < len(candles):
            candle = candles[i]
            is_last = (i == len(candles) - 1)
            time_str = self._candle_time(candle)

            if position:
                # ── SL check (checked before target) ──
                sl_hit = False
                if (position["direction"] == "BUY"
                        and candle["low"] <= position["sl_price"]):
                    sl_hit = True
                elif (position["direction"] == "SELL"
                      and candle["high"] >= position["sl_price"]):
                    sl_hit = True

                if sl_hit:
                    trades.append(self._record_trade(
                        position, position["sl_price"], time_str,
                        symbol, lot_size, position["remaining_lots"], "SL",
                    ))
                    old_dir = position["direction"]
                    position = None

                    if self._cutoff_hit(trades, cutoff):
                        break

                    ctx = StrategyContext(
                        client=self.client, settings=settings,
                        trading_symbol=symbol, current_direction=old_dir,
                    )
                    result = strategy.on_sl_hit(ctx, strategy_data)
                    action = result.get("action", "stop")
                    new_dir = result.get("direction", "")

                    if action == "reverse":
                        entry = candle["close"]
                        if entry >= entry_floor:
                            position = self._make_position(
                                new_dir, entry, settings,
                                time_str, settings.quantity,
                            )
                        else:
                            pending_direction = new_dir
                        i += 1
                        continue

                    elif action == "reselect_and_enter":
                        new_opt = strategy_data.get("option_type", "CE")
                        # Use NIFTY spot at current time for ATM calculation
                        # and check premium at current candle time
                        current_cdt = candle["date"]
                        if current_cdt.tzinfo is None:
                            current_cdt = IST.localize(current_cdt)
                        current_spot = None
                        if nifty_candles:
                            current_spot = self._get_spot_at_time(
                                nifty_candles, current_cdt,
                            )
                        new_inst, new_candles, err = self._auto_select_instrument(
                            all_instruments, new_opt, date_str,
                            settings.min_premium,
                            settings.start_time, settings.stop_time,
                            check_time=current_cdt,
                            spot_override=current_spot,
                            expiry_type=settings.expiry_type,
                        )
                        if err:
                            logger.warning(f"Reselect failed: {err}")
                            i += 1
                            continue

                        # Find candle at or after the current time
                        new_i = len(new_candles)
                        for j, nc in enumerate(new_candles):
                            nc_dt = nc["date"]
                            if nc_dt.tzinfo is None:
                                nc_dt = IST.localize(nc_dt)
                            if nc_dt >= current_cdt:
                                new_i = j
                                break

                        if new_i >= len(new_candles):
                            i += 1
                            continue

                        # Switch instruments and candle stream
                        candles = new_candles
                        current_instrument = new_inst
                        symbol = new_inst["tradingsymbol"]
                        lot_size = int(new_inst.get("lot_size", 1))

                        entry_candle = candles[new_i]
                        entry = entry_candle["close"]
                        entry_time = self._candle_time(entry_candle)

                        if entry >= entry_floor:
                            position = self._make_position(
                                new_dir, entry, settings,
                                entry_time, settings.quantity,
                            )
                        else:
                            pending_direction = new_dir
                        i = new_i + 1
                        continue

                    else:  # stop
                        break

                # ── Target check with trailing ──
                targets_crossed = 0
                exit_levels = []

                if position["direction"] == "BUY":
                    while candle["high"] >= position["target_price"]:
                        exit_levels.append(position["target_price"])
                        position["target_price"] += settings.target_points
                        position["sl_price"] += settings.sl_points
                        targets_crossed += 1
                elif position["direction"] == "SELL":
                    while candle["low"] <= position["target_price"]:
                        exit_levels.append(position["target_price"])
                        position["target_price"] -= settings.target_points
                        position["sl_price"] -= settings.sl_points
                        targets_crossed += 1

                # Partial exits on each target crossed
                for k in range(targets_crossed):
                    ctx = StrategyContext(
                        client=self.client, settings=settings,
                        trading_symbol=symbol,
                        current_direction=position["direction"],
                    )
                    result = strategy.on_target_hit(
                        ctx, strategy_data, position["remaining_lots"],
                    )
                    if (result.get("action") == "partial_exit"
                            and position["remaining_lots"] > 1):
                        exit_n = min(
                            result.get("exit_lots", 1),
                            position["remaining_lots"] - 1,
                        )
                        trades.append(self._record_trade(
                            position, exit_levels[k], time_str,
                            symbol, lot_size, exit_n, "Partial",
                        ))
                        position["remaining_lots"] -= exit_n

                # ── Square off at last candle ──
                if is_last and position:
                    trades.append(self._record_trade(
                        position, candle["close"], time_str,
                        symbol, lot_size, position["remaining_lots"], "EOD",
                    ))
                    position = None

            else:
                # ── No position: enter at candle close ──
                if not is_last and not self._cutoff_hit(trades, cutoff):
                    close = candle["close"]
                    if close < entry_floor:
                        i += 1
                        continue

                    if pending_direction:
                        direction = pending_direction
                        pending_direction = None
                    else:
                        direction = strategy.initial_direction(
                            strategy_data, settings.market_bias,
                        )

                    position = self._make_position(
                        direction, close, settings, time_str, settings.quantity,
                    )

            i += 1

        return trades

    # ── Candle-SL Simulation (Alternate — no scale out) ─────────────────

    def _simulate_candle_alt(self, candles, strategy, strategy_data, settings,
                             current_instrument, all_instruments, date_str,
                             nifty_candles=None, cutoff=None):
        """Simulate candle-based SL for alternate strategy: exit at N-min close
        when (high_since_entry - close) > sl_points, then reverse.
        No scale out, no target/trailing."""
        trades = []
        position = None
        pending_direction = None
        symbol = current_instrument["tradingsymbol"]
        lot_size = int(current_instrument.get("lot_size", 1))
        entry_floor = MIN_PREMIUM
        bar_min = getattr(strategy, 'candle_sl_minutes', 5)
        high_since_entry = 0.0

        pending_direction = strategy.initial_direction(
            strategy_data, settings.market_bias,
        )

        i = 0
        while i < len(candles):
            candle = candles[i]
            is_last = (i == len(candles) - 1)
            time_str = self._candle_time(candle)

            if position:
                # Track high since entry
                high_since_entry = max(high_since_entry, candle["high"])

                # Check at N-min bar close or last candle
                cdt = candle["date"]
                is_bar_close = (cdt.minute % bar_min == bar_min - 1) or is_last

                if is_bar_close:
                    close = candle["close"]
                    drop = high_since_entry - close

                    if drop > settings.sl_points and not is_last:
                        # Candle SL hit — exit and reverse
                        trades.append(self._record_trade(
                            position, close, time_str,
                            symbol, lot_size, position["remaining_lots"], "SL",
                        ))
                        old_dir = position["direction"]
                        position = None

                        if self._cutoff_hit(trades, cutoff):
                            break

                        ctx = StrategyContext(
                            client=self.client, settings=settings,
                            trading_symbol=symbol, current_direction=old_dir,
                        )
                        result = strategy.on_sl_hit(ctx, strategy_data)
                        action = result.get("action", "stop")
                        new_dir = result.get("direction", "")

                        if action == "reselect_and_enter":
                            new_opt = strategy_data.get("option_type", "CE")
                            current_cdt = cdt
                            if current_cdt.tzinfo is None:
                                current_cdt = IST.localize(current_cdt)
                            current_spot = None
                            if nifty_candles:
                                current_spot = self._get_spot_at_time(
                                    nifty_candles, current_cdt,
                                )
                            new_inst, new_candles, err = self._auto_select_instrument(
                                all_instruments, new_opt, date_str,
                                settings.min_premium,
                                settings.start_time, settings.stop_time,
                                check_time=current_cdt,
                                spot_override=current_spot,
                                expiry_type=settings.expiry_type,
                            )
                            if err:
                                logger.warning(f"Reselect failed: {err}")
                                i += 1
                                continue

                            new_i = len(new_candles)
                            for j, nc in enumerate(new_candles):
                                nc_dt = nc["date"]
                                if nc_dt.tzinfo is None:
                                    nc_dt = IST.localize(nc_dt)
                                if nc_dt >= current_cdt:
                                    new_i = j
                                    break

                            # Enter at next candle's open (simulate execution delay)
                            entry_idx = new_i + 1
                            if entry_idx >= len(new_candles):
                                i = len(new_candles)
                                continue

                            candles = new_candles
                            current_instrument = new_inst
                            symbol = new_inst["tradingsymbol"]
                            lot_size = int(new_inst.get("lot_size", 1))

                            entry_candle = candles[entry_idx]
                            entry = entry_candle["open"]
                            entry_time = self._candle_time(entry_candle)

                            if entry >= entry_floor:
                                position = self._make_position(
                                    new_dir, entry, settings,
                                    entry_time, settings.quantity,
                                )
                                high_since_entry = entry
                            else:
                                pending_direction = new_dir
                            i = entry_idx + 1
                            continue

                        elif action == "reverse":
                            entry = close
                            if entry >= entry_floor:
                                position = self._make_position(
                                    new_dir, entry, settings,
                                    time_str, settings.quantity,
                                )
                                high_since_entry = entry
                            else:
                                pending_direction = new_dir
                            i += 1
                            continue

                        else:  # stop
                            break

                # ── Square off at last candle ──
                if is_last and position:
                    trades.append(self._record_trade(
                        position, candle["close"], time_str,
                        symbol, lot_size, position["remaining_lots"], "EOD",
                    ))
                    position = None

            else:
                # ── No position: enter at candle close ──
                if not is_last and not self._cutoff_hit(trades, cutoff):
                    close = candle["close"]
                    if close < entry_floor:
                        i += 1
                        continue

                    if pending_direction:
                        direction = pending_direction
                        pending_direction = None
                    else:
                        direction = strategy.initial_direction(
                            strategy_data, settings.market_bias,
                        )

                    position = self._make_position(
                        direction, close, settings, time_str, settings.quantity,
                    )
                    high_since_entry = close

            i += 1

        return trades

    # ── Candle-SL Simulation (Scale Out) ──────────────────────────────────

    def _simulate_candle_scaleout(self, candles, strategy, strategy_data,
                                  settings, current_instrument,
                                  all_instruments, date_str,
                                  nifty_candles=None, cutoff=None):
        """Simulate candle-based SL for scale-out strategy: enters multiple lots,
        scales out 1 lot at each target hit (checked at N-min close),
        exits all remaining when (high_since_entry - close) > sl_points."""
        trades = []
        position = None
        pending_direction = None
        symbol = current_instrument["tradingsymbol"]
        lot_size = int(current_instrument.get("lot_size", 1))
        entry_floor = MIN_PREMIUM
        bar_min = getattr(strategy, 'candle_sl_minutes', 5)
        high_since_entry = 0.0

        pending_direction = strategy.initial_direction(
            strategy_data, settings.market_bias,
        )

        i = 0
        while i < len(candles):
            candle = candles[i]
            is_last = (i == len(candles) - 1)
            time_str = self._candle_time(candle)

            if position:
                # Track high since entry
                high_since_entry = max(high_since_entry, candle["high"])

                # Check SL at every 1-min candle close
                cdt = candle["date"]
                close = candle["close"]
                drop = high_since_entry - close

                if drop > settings.sl_points and not is_last:
                    # Candle SL hit — exit all remaining and reverse
                    trades.append(self._record_trade(
                        position, close, time_str,
                        symbol, lot_size, position["remaining_lots"], "SL",
                    ))
                    old_dir = position["direction"]
                    position = None

                    if self._cutoff_hit(trades, cutoff):
                        break

                    ctx = StrategyContext(
                        client=self.client, settings=settings,
                        trading_symbol=symbol, current_direction=old_dir,
                    )
                    result = strategy.on_sl_hit(ctx, strategy_data)
                    action = result.get("action", "stop")
                    new_dir = result.get("direction", "")

                    if action == "reselect_and_enter":
                        new_opt = strategy_data.get("option_type", "CE")
                        current_cdt = cdt
                        if current_cdt.tzinfo is None:
                            current_cdt = IST.localize(current_cdt)
                        current_spot = None
                        if nifty_candles:
                            current_spot = self._get_spot_at_time(
                                nifty_candles, current_cdt,
                            )
                        new_inst, new_candles, err = self._auto_select_instrument(
                            all_instruments, new_opt, date_str,
                            settings.min_premium,
                            settings.start_time, settings.stop_time,
                            check_time=current_cdt,
                            spot_override=current_spot,
                            expiry_type=settings.expiry_type,
                        )
                        if err:
                            logger.warning(f"Reselect failed: {err}")
                            i += 1
                            continue

                        new_i = len(new_candles)
                        for j, nc in enumerate(new_candles):
                            nc_dt = nc["date"]
                            if nc_dt.tzinfo is None:
                                nc_dt = IST.localize(nc_dt)
                            if nc_dt >= current_cdt:
                                new_i = j
                                break

                        if new_i >= len(new_candles):
                            i += 1
                            continue

                        candles = new_candles
                        current_instrument = new_inst
                        symbol = new_inst["tradingsymbol"]
                        lot_size = int(new_inst.get("lot_size", 1))

                        entry_candle = candles[new_i]
                        entry = entry_candle["close"]
                        entry_time = self._candle_time(entry_candle)

                        if entry >= entry_floor:
                            position = self._make_position(
                                new_dir, entry, settings,
                                entry_time, settings.quantity,
                            )
                            high_since_entry = entry
                        else:
                            pending_direction = new_dir
                        i = new_i + 1
                        continue

                    elif action == "reverse":
                        entry = close
                        if entry >= entry_floor:
                            position = self._make_position(
                                new_dir, entry, settings,
                                time_str, settings.quantity,
                            )
                            high_since_entry = entry
                        else:
                            pending_direction = new_dir
                        i += 1
                        continue

                    else:  # stop
                        break

                # ── Scale out at bar close ──
                is_bar_close = (cdt.minute % bar_min == bar_min - 1) or is_last
                if is_bar_close and position:
                    exit_per_target = settings.quantity // strategy.lot_multiplier
                    if position["remaining_lots"] > exit_per_target:
                        while (close >= position["target_price"]
                               and position["remaining_lots"] > exit_per_target):
                            trades.append(self._record_trade(
                                position, position["target_price"], time_str,
                                symbol, lot_size, exit_per_target, "Partial",
                            ))
                            position["remaining_lots"] -= exit_per_target
                            position["target_price"] += settings.target_points
                            position["sl_price"] += settings.sl_points

                # ── Square off at last candle ──
                if is_last and position:
                    trades.append(self._record_trade(
                        position, candle["close"], time_str,
                        symbol, lot_size, position["remaining_lots"], "EOD",
                    ))
                    position = None

            else:
                # ── No position: enter at candle close ──
                if not is_last and not self._cutoff_hit(trades, cutoff):
                    close = candle["close"]
                    if close < entry_floor:
                        i += 1
                        continue

                    if pending_direction:
                        direction = pending_direction
                        pending_direction = None
                    else:
                        direction = strategy.initial_direction(
                            strategy_data, settings.market_bias,
                        )

                    position = self._make_position(
                        direction, close, settings, time_str, settings.quantity,
                    )
                    high_since_entry = close

            i += 1

        return trades
