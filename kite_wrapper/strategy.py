"""Pluggable Trading Strategy Engine."""

import json
import logging
import threading
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Optional

import pytz

from .client import KiteClient
from .base_strategy import BaseStrategy, StrategyContext, STRATEGY_REGISTRY

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")
STATE_FILE = Path(__file__).parent.parent / ".strategy_state.json"
HISTORY_FILE = Path(__file__).parent.parent / ".trade_history.jsonl"


# ─── Data Models ────────────────────────────────────────────────────────────


@dataclass
class StrategySettings:
    enabled: bool = False
    start_time: str = "10:00"
    stop_time: str = "15:15"
    sl_points: float = 10.0
    target_points: float = 10.0
    quantity: int = 0  # lots (auto-set from lot_multiplier on instrument selection)
    product: str = "NRML"
    market_bias: str = "BULLISH"  # BULLISH or BEARISH
    min_premium: float = 100.0  # minimum option premium for instrument selection
    expiry_type: str = "weekly"  # "weekly" or "monthly"
    daily_cutoff: bool = False
    daily_profit_pct: float = 25.0
    daily_loss_pct: float = 25.0


@dataclass
class ActivePosition:
    direction: str = ""  # "SELL" or "BUY"
    entry_price: float = 0.0
    sl_price: float = 0.0
    target_price: float = 0.0
    sl_order_id: str = ""
    order_id: str = ""
    entry_time: str = ""
    remaining_lots: int = 0  # 0 = not set (backward compat), >0 = lots held


@dataclass
class TradeRecord:
    direction: str = ""
    entry_price: float = 0.0
    exit_price: float = 0.0
    entry_time: str = ""
    exit_time: str = ""
    pnl: float = 0.0
    quantity: int = 0
    date: str = ""
    symbol: str = ""


@dataclass
class PositionSlot:
    """One independent position slot for signal-based strategies (CE or PE)."""
    option_type: str = ""
    instrument_token: int = 0
    trading_symbol: str = ""
    lot_size: int = 0
    active_position: Optional[ActivePosition] = None
    current_ltp: float = 0.0
    pending_partial_exits: int = 0


@dataclass
class StrategyState:
    settings: StrategySettings = field(default_factory=StrategySettings)
    engine_status: str = "STOPPED"
    status_message: str = ""
    trading_symbol: str = ""
    instrument_token: int = 0
    lot_size: int = 0
    active_position: Optional[ActivePosition] = None
    trades_today: list = field(default_factory=list)
    total_pnl: float = 0.0
    current_ltp: float = 0.0
    last_date: str = ""
    strategy_name: str = "sar"
    strategy_data: dict = field(default_factory=dict)
    # Dual-position slots (only used by signal_based strategies)
    position_slots: dict = field(default_factory=dict)  # {"CE": PositionSlot, "PE": PositionSlot}


# ─── Serialization Helpers ──────────────────────────────────────────────────


def _slot_to_dict(slot: PositionSlot) -> dict:
    return {
        "option_type": slot.option_type,
        "instrument_token": slot.instrument_token,
        "trading_symbol": slot.trading_symbol,
        "lot_size": slot.lot_size,
        "active_position": asdict(slot.active_position) if slot.active_position else None,
        "current_ltp": slot.current_ltp,
        "pending_partial_exits": slot.pending_partial_exits,
    }


def _slot_from_dict(d: dict) -> PositionSlot:
    slot = PositionSlot()
    slot.option_type = d.get("option_type", "")
    slot.instrument_token = d.get("instrument_token", 0)
    slot.trading_symbol = d.get("trading_symbol", "")
    slot.lot_size = d.get("lot_size", 0)
    if d.get("active_position"):
        slot.active_position = ActivePosition(**d["active_position"])
    slot.current_ltp = d.get("current_ltp", 0.0)
    slot.pending_partial_exits = d.get("pending_partial_exits", 0)
    return slot


def _state_to_dict(state: StrategyState) -> dict:
    return {
        "settings": asdict(state.settings),
        "engine_status": state.engine_status,
        "status_message": state.status_message,
        "trading_symbol": state.trading_symbol,
        "instrument_token": state.instrument_token,
        "lot_size": state.lot_size,
        "active_position": asdict(state.active_position) if state.active_position else None,
        "trades_today": [
            asdict(t) if isinstance(t, TradeRecord) else t
            for t in state.trades_today
        ],
        "total_pnl": state.total_pnl,
        "current_ltp": state.current_ltp,
        "last_date": state.last_date,
        "strategy_name": state.strategy_name,
        "strategy_data": state.strategy_data,
        "position_slots": {
            k: _slot_to_dict(v) for k, v in state.position_slots.items()
        },
    }


def _state_from_dict(d: dict) -> StrategyState:
    state = StrategyState()
    if "settings" in d:
        state.settings = StrategySettings(**d["settings"])
    state.engine_status = d.get("engine_status", "STOPPED")
    state.status_message = d.get("status_message", "")
    state.trading_symbol = d.get("trading_symbol", "")
    state.instrument_token = d.get("instrument_token", 0)
    state.lot_size = d.get("lot_size", 0)
    if d.get("active_position"):
        state.active_position = ActivePosition(**d["active_position"])
    state.trades_today = [
        TradeRecord(**t) if isinstance(t, dict) else t
        for t in d.get("trades_today", [])
    ]
    state.total_pnl = d.get("total_pnl", 0.0)
    state.current_ltp = d.get("current_ltp", 0.0)
    state.last_date = d.get("last_date", "")
    state.strategy_name = d.get("strategy_name", "sar")
    state.strategy_data = d.get("strategy_data", {})
    state.position_slots = {
        k: _slot_from_dict(v) for k, v in d.get("position_slots", {}).items()
    }
    return state


# ─── Strategy Engine ────────────────────────────────────────────────────────


class StrategyEngine:
    """Pluggable trading engine — delegates strategy decisions to a BaseStrategy."""

    def __init__(self, client: KiteClient):
        self.client = client
        self._lock = threading.Lock()
        self._thread: Optional[threading.Thread] = None
        self._running = False
        self._ticker = None
        self._reversal_in_progress = False
        self._pending_direction: Optional[str] = None
        self._pending_partial_exits = 0
        self._recovery_mode = False  # True after restart — only monitor, no new orders
        self._tick_ready = False  # True after ticker on_connect fires
        self._sl_breach_time: dict = {}  # opt_type → time when SL breach first detected
        self._strategy: Optional[BaseStrategy] = None

        self.state = self._load_state()
        self._set_strategy(self.state.strategy_name)

    # ── State Persistence ───────────────────────────────────────────────

    @staticmethod
    def _load_todays_trades() -> tuple[list, float]:
        """Rebuild today's trades and P&L from the JSONL history file."""
        today_str = date.today().isoformat()
        trades = []
        total_pnl = 0.0
        if HISTORY_FILE.exists():
            try:
                for line in HISTORY_FILE.read_text().splitlines():
                    line = line.strip()
                    if not line:
                        continue
                    t = json.loads(line)
                    if t.get("date") == today_str:
                        trades.append(TradeRecord(**t))
                        total_pnl += t.get("pnl", 0.0)
            except Exception as e:
                logger.error(f"Failed to load today's trades from history: {e}")
        return trades, round(total_pnl, 2)

    def _load_state(self) -> StrategyState:
        if STATE_FILE.exists():
            try:
                data = json.loads(STATE_FILE.read_text())
                state = _state_from_dict(data)
                # Always start clean — only keep settings, strategy_name, lot_size
                state.active_position = None
                state.trading_symbol = ""
                state.instrument_token = 0
                state.strategy_data = {}
                state.current_ltp = 0.0
                state.position_slots = {}
                state.last_date = date.today().isoformat()
                state.engine_status = "STOPPED"
                state.status_message = ""
                # Rebuild today's P&L from history (survives restarts)
                state.trades_today, state.total_pnl = self._load_todays_trades()
                return state
            except Exception as e:
                logger.error(f"Failed to load state: {e}")
        state = StrategyState()
        state.last_date = date.today().isoformat()
        state.trades_today, state.total_pnl = self._load_todays_trades()
        return state

    def _save_state(self):
        """Save state to JSON file. Must be called with lock held."""
        try:
            STATE_FILE.write_text(json.dumps(_state_to_dict(self.state), indent=2))
        except Exception as e:
            logger.error(f"Failed to save state: {e}")

    def get_snapshot(self) -> dict:
        """Thread-safe snapshot for the dashboard API."""
        with self._lock:
            return _state_to_dict(self.state)

    # ── Strategy Selection ─────────────────────────────────────────────

    def _set_strategy(self, name: str):
        """Instantiate and store the strategy from the registry."""
        cls = STRATEGY_REGISTRY.get(name)
        if cls is None:
            logger.warning(f"Unknown strategy '{name}', falling back to 'sar'")
            cls = STRATEGY_REGISTRY.get("sar")
        if cls is None:
            raise RuntimeError("No strategies registered — import kite_wrapper.strategies")
        self._strategy = cls()
        logger.info(f"Strategy set: {name}")

    def switch_strategy(self, name: str):
        """Switch strategy (only when engine is stopped)."""
        if self._running:
            raise RuntimeError("Cannot switch strategy while engine is running")
        if name not in STRATEGY_REGISTRY:
            raise ValueError(f"Unknown strategy: {name}")
        self._set_strategy(name)
        with self._lock:
            self.state.strategy_name = name
            self.state.strategy_data = {}
            self._save_state()

    def _make_context(self, current_direction: str = "") -> StrategyContext:
        """Build a StrategyContext snapshot for strategy method calls."""
        return StrategyContext(
            client=self.client,
            settings=self.state.settings,
            trading_symbol=self.state.trading_symbol,
            current_direction=current_direction,
        )

    # ── Instrument Selection ────────────────────────────────────────────

    def _select_instrument(self) -> bool:
        """Delegate instrument selection to the active strategy, then apply state."""
        ctx = self._make_context()
        chosen = self._strategy.select_instrument(ctx, self.state.strategy_data)
        if chosen is None:
            return False

        today = date.today()
        lot = int(chosen.get("lot_size", 1))
        expiry = chosen["expiry"]

        # Force NRML for far-expiry contracts
        next_month_end = (today.replace(day=28) + timedelta(days=35)).replace(day=1)
        product_override = "NRML" if expiry >= next_month_end else None

        with self._lock:
            self.state.trading_symbol = chosen["tradingsymbol"]
            self.state.instrument_token = chosen["instrument_token"]
            self.state.lot_size = lot
            if product_override and self.state.settings.product != product_override:
                logger.info(
                    f"Switching product → {product_override} "
                    f"(expiry {expiry} too far for MIS)"
                )
                self.state.settings.product = product_override
            min_lots = self._strategy.lot_multiplier
            if self.state.settings.quantity < min_lots or self.state.settings.quantity % min_lots != 0:
                self.state.settings.quantity = min_lots
            self._save_state()

        return True

    # ── Order Management ────────────────────────────────────────────────

    # NIFTY option tick size
    TICK = 0.05

    @staticmethod
    def _entry_buffer(ltp: float) -> float:
        """Entry limit-price buffer: 2% of LTP, clamped between 0.5 and 3."""
        return max(0.5, min(round(ltp * 0.02, 2), 3.0))

    @staticmethod
    def _sl_buffer(trigger_price: float) -> float:
        """SL limit-price buffer: 3 pt gap so the order fills even in fast markets."""
        return 3.0

    @staticmethod
    def _round_tick(price: float) -> float:
        """Round to nearest valid tick (0.05)."""
        return round(round(price / StrategyEngine.TICK) * StrategyEngine.TICK, 2)

    def _get_ltp(self) -> float:
        """Get current LTP from ticker state, falling back to REST API."""
        ltp = self.state.current_ltp
        if ltp > 0:
            return ltp
        try:
            sym = f"NFO:{self.state.trading_symbol}"
            data = self.client.kite.ltp(sym)
            return data[sym]["last_price"]
        except Exception as e:
            logger.error(f"Failed to get LTP: {e}")
            return 0.0

    def _place_entry_order(self, direction: str, quantity: int = 0) -> Optional[str]:
        """Place a LIMIT order near LTP. Returns order_id or None."""
        qty = quantity or (self.state.settings.quantity * self.state.lot_size)
        ltp = self._get_ltp()
        if ltp <= 0:
            logger.error("Cannot place entry: no LTP available")
            return None

        # Aggressive limit to ensure quick fill
        buf = self._entry_buffer(ltp)
        if direction == "SELL":
            price = self._round_tick(ltp - buf)
        else:
            price = self._round_tick(ltp + buf)

        try:
            order_id = self.client.kite.place_order(
                variety="regular",
                exchange="NFO",
                tradingsymbol=self.state.trading_symbol,
                transaction_type=direction,
                quantity=qty,
                product=self.state.settings.product,
                order_type="LIMIT",
                price=price,
            )
            logger.info(
                f"Entry order: {direction} {self.state.trading_symbol} "
                f"LIMIT@{price}, order_id={order_id}"
            )
            return str(order_id)
        except Exception as e:
            logger.error(f"Entry order failed: {e}")
            return None

    def _get_fill_price(self, order_id: str, timeout: int = 60, cancel_on_timeout: bool = False) -> Optional[float]:
        """Poll order history until filled or failed."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                history = self.client.kite.order_history(order_id)
                for update in reversed(history):
                    if update["status"] == "COMPLETE":
                        return float(update["average_price"])
                    if update["status"] in ("CANCELLED", "REJECTED"):
                        logger.error(
                            f"Order {order_id} {update['status']}: "
                            f"{update.get('status_message', '')}"
                        )
                        return None
            except Exception as e:
                logger.error(f"Error polling order {order_id}: {e}")
            time.sleep(1)
        if cancel_on_timeout:
            self._cancel_order(order_id)
        return None

    def _cancel_open_orders(self):
        """Cancel all open/trigger-pending orders for our trading symbol.
        Frees up margin blocked by stale orders from earlier attempts."""
        symbol = self.state.trading_symbol
        if not symbol:
            return
        try:
            orders = self.client.kite.orders()
            for o in orders:
                if (o["tradingsymbol"] == symbol
                        and o["status"] in ("OPEN", "TRIGGER PENDING")):
                    try:
                        self.client.kite.cancel_order(
                            variety=o.get("variety", "regular"),
                            order_id=o["order_id"],
                        )
                        logger.info(f"Cancelled stale order {o['order_id']} "
                                    f"({o['transaction_type']} {o['status']})")
                    except Exception as e:
                        logger.warning(f"Failed to cancel order {o['order_id']}: {e}")
        except Exception as e:
            logger.error(f"Failed to fetch orders for cleanup: {e}")

    def _place_sl_order(self, direction: str, trigger_price: float, quantity: int = 0) -> Optional[str]:
        """Place SL (stop-loss limit) protection order on exchange."""
        qty = quantity or (self.state.settings.quantity * self.state.lot_size)
        trigger_price = self._round_tick(trigger_price)
        # SELL position → BUY SL, BUY position → SELL SL
        sl_side = "BUY" if direction == "SELL" else "SELL"

        # Proportional limit buffer so the order fills even on a gap
        buf = self._sl_buffer(trigger_price)
        if sl_side == "BUY":
            price = self._round_tick(trigger_price + buf)
        else:
            price = self._round_tick(trigger_price - buf)

        try:
            order_id = self.client.kite.place_order(
                variety="regular",
                exchange="NFO",
                tradingsymbol=self.state.trading_symbol,
                transaction_type=sl_side,
                quantity=qty,
                product=self.state.settings.product,
                order_type="SL",
                trigger_price=trigger_price,
                price=price,
            )
            logger.info(
                f"SL order: {sl_side} trigger={trigger_price} limit={price}, "
                f"order_id={order_id}"
            )
            return str(order_id)
        except Exception as e:
            logger.error(f"SL order failed: {e}")
            return None

    def _modify_sl_order(self, order_id: str, new_trigger: float) -> bool:
        """Trail existing SL order to a new trigger + limit price."""
        new_trigger = self._round_tick(new_trigger)
        with self._lock:
            pos = self.state.active_position
            if not pos:
                return False
            direction = pos.direction

        sl_side = "BUY" if direction == "SELL" else "SELL"
        buf = self._sl_buffer(new_trigger)
        if sl_side == "BUY":
            new_price = self._round_tick(new_trigger + buf)
        else:
            new_price = self._round_tick(new_trigger - buf)

        try:
            self.client.kite.modify_order(
                variety="regular",
                order_id=order_id,
                trigger_price=new_trigger,
                price=new_price,
            )
            logger.info(
                f"SL modified: order_id={order_id}, "
                f"trigger={new_trigger}, limit={new_price}"
            )
            return True
        except Exception as e:
            logger.error(f"SL modify failed: {e}")
            return False

    def _cancel_order(self, order_id: str) -> bool:
        try:
            self.client.kite.cancel_order(variety="regular", order_id=order_id)
            logger.info(f"Order cancelled: {order_id}")
            return True
        except Exception as e:
            logger.error(f"Order cancel failed: {e}")
            return False

    # ── SL Order Monitoring ─────────────────────────────────────────────

    def _manual_sl_exit(self, symbol: str, direction: str, quantity: int) -> Optional[float]:
        """Cancel unfilled SL and manually exit with retry at current LTP.

        Returns fill price or None.
        """
        close_dir = "SELL" if direction == "BUY" else "BUY"
        fill_price = None
        max_attempts = 5

        for attempt in range(1, max_attempts + 1):
            # Get fresh LTP
            try:
                sym = f"NFO:{symbol}"
                data = self.client.kite.ltp(sym)
                ltp = data[sym]["last_price"]
            except Exception:
                ltp = 0.0
            if ltp <= 0:
                time.sleep(1)
                continue

            buf = self._entry_buffer(ltp)
            if close_dir == "SELL":
                price = self._round_tick(ltp - buf)
            else:
                price = self._round_tick(ltp + buf)

            try:
                order_id = self.client.kite.place_order(
                    variety="regular", exchange="NFO",
                    tradingsymbol=symbol,
                    transaction_type=close_dir, quantity=quantity,
                    product=self.state.settings.product,
                    order_type="LIMIT", price=price,
                )
                order_id = str(order_id)
                logger.info(
                    f"Manual SL exit: {close_dir} {symbol} LIMIT@{price}, "
                    f"order_id={order_id} (attempt {attempt}/{max_attempts})"
                )
            except Exception as e:
                logger.error(f"Manual SL exit order failed: {e}")
                if attempt < max_attempts:
                    time.sleep(1)
                continue

            fill_price = self._get_fill_price(order_id, timeout=3)
            if fill_price is not None:
                break

            self._cancel_order(order_id)
            if attempt < max_attempts:
                logger.info(f"Manual SL exit not filled (attempt {attempt}/{max_attempts}), retrying")

        return fill_price

    def _monitor_sl_fill(self):
        """Single-position mode: detect SL breach and manually exit if SL order didn't fill."""
        with self._lock:
            pos = self.state.active_position
            if not pos or not pos.sl_order_id:
                self._sl_breach_time.pop("single", None)
                return
            ltp = self.state.current_ltp
            if ltp <= 0:
                return
            sl_price = pos.sl_price
            direction = pos.direction
            sl_order_id = pos.sl_order_id
            symbol = self.state.trading_symbol
            qty = pos.remaining_lots * self.state.lot_size

        # Check if LTP has crossed SL level
        sl_breached = False
        if direction == "BUY" and ltp <= sl_price:
            sl_breached = True
        elif direction == "SELL" and ltp >= sl_price:
            sl_breached = True

        if not sl_breached:
            self._sl_breach_time.pop("single", None)
            return

        # First detection — record time, give exchange 3s to fill
        now = time.time()
        if "single" not in self._sl_breach_time:
            self._sl_breach_time["single"] = now
            logger.warning(f"SL breach detected: LTP={ltp}, SL={sl_price}. Monitoring...")
            return

        # Wait at least 3 seconds before taking manual action
        if now - self._sl_breach_time["single"] < 3:
            return

        self._sl_breach_time.pop("single", None)

        # Check if SL order already filled (order update may have handled it)
        with self._lock:
            if not self.state.active_position:
                return  # Already closed by order update

        # SL still not filled after 3s — check order status
        try:
            history = self.client.kite.order_history(sl_order_id)
            latest = history[-1] if history else {}
            status = latest.get("status", "")
        except Exception as e:
            logger.error(f"SL status check failed: {e}")
            return

        if status == "COMPLETE":
            return  # Order update callback will handle it

        # SL not filled — cancel and manually exit
        logger.warning(
            f"SL not filled after breach (status={status}, LTP={ltp}, SL={sl_price}). "
            f"Cancelling SL and placing manual exit."
        )
        self._cancel_order(sl_order_id)

        fill_price = self._manual_sl_exit(symbol, direction, qty)
        if fill_price is None:
            logger.error("Manual SL exit failed — position still open!")
            return

        logger.info(f"Manual SL exit filled @ {fill_price}")
        self._reversal_in_progress = True
        try:
            self._handle_sl_hit(fill_price)
        finally:
            self._reversal_in_progress = False

    def _check_candle_exit_alt(self):
        """Candle-based SL for alternate strategy: at N-min boundary,
        exit if (high_since_entry - bar_close) > sl_points. No scale out.
        Uses 1-min candles for both high and close to match backtest exactly."""
        now = datetime.now(IST)
        bar_min = getattr(self._strategy, 'candle_sl_minutes', 5)
        if now.minute % bar_min != 0:
            return
        check_key = f"{now.hour}:{now.minute}"
        if self.state.strategy_data.get("_candle_exit_check") == check_key:
            return
        self.state.strategy_data["_candle_exit_check"] = check_key

        with self._lock:
            pos = self.state.active_position
            if not pos:
                return
            sl_pts = self.state.settings.sl_points
            token = self.state.instrument_token
            entry_time = pos.entry_time  # "HH:MM:SS"

        # Fetch 1-min candles for high and close
        try:
            today = now.date()
            candles = self.client.kite.historical_data(
                token, today, today, "minute"
            )
        except Exception as e:
            logger.error(f"Candle exit check — failed to fetch candles: {e}")
            return

        if not candles:
            return

        # Max high from 1-min candles since entry (matches backtest)
        high_since_entry = pos.entry_price
        for c in candles:
            cdt = c["date"]
            c_time = cdt.strftime("%H:%M:%S") if hasattr(cdt, 'strftime') else str(cdt)
            if c_time > entry_time:
                high_since_entry = max(high_since_entry, c["high"])

        # Use last completed 1-min candle's close as bar close
        bar_close = candles[-1]["close"]
        drop = high_since_entry - bar_close

        logger.info(
            f"Candle exit check: high={high_since_entry:.2f}, "
            f"bar_close={bar_close:.2f}, drop={drop:.2f}, sl={sl_pts}"
        )

        if drop > sl_pts:
            logger.info(
                f"Candle SL triggered: drop {drop:.2f} > {sl_pts}, "
                f"exiting at market"
            )
            self._reversal_in_progress = True
            try:
                with self._lock:
                    symbol = self.state.trading_symbol
                    direction = pos.direction
                    remaining_lots = pos.remaining_lots if pos.remaining_lots > 0 else self.state.settings.quantity
                    quantity = remaining_lots * self.state.lot_size

                fill_price = self._manual_sl_exit(symbol, direction, quantity)
                if fill_price is None:
                    logger.error("Candle SL — exit failed after 5 attempts. Will retry next check.")
                    return
                self._close_position(exit_price=fill_price)
                logger.info(f"Candle SL exit filled @ {fill_price}")

                ctx = self._make_context(current_direction=pos.direction)
                result = self._strategy.on_sl_hit(ctx, self.state.strategy_data)
                action = result.get("action", "stop")
                new_direction = result.get("direction", "")

                if action == "reselect_and_enter":
                    logger.info(f"Candle SL → reselecting and entering {new_direction}")
                    self._stop_ticker()
                    with self._lock:
                        self.state.trading_symbol = ""
                        self.state.instrument_token = 0
                        self.state.current_ltp = 0.0
                        self._save_state()
                    if self._select_instrument():
                        self._start_ticker()
                        time.sleep(2)
                        self._pending_direction = new_direction
                        self._enter_position(new_direction)
                elif action == "reverse":
                    logger.info(f"Candle SL → reversing to {new_direction}")
                    self._pending_direction = new_direction
                    self._enter_position(new_direction)
                else:
                    logger.info("Candle SL → strategy chose to stop")
                    self._stop_ticker()
                    self._running = False
                    with self._lock:
                        self.state.engine_status = "STOPPED"
                        self.state.status_message = "Stopped after candle SL"
                        self._save_state()
            finally:
                self._reversal_in_progress = False

    def _check_candle_exit_scaleout(self):
        """Candle-based SL for scale-out strategy: at N-min boundary,
        scale out 1 group at each target hit, exit all remaining when
        (high_since_entry - bar_close) > sl_points.
        Uses 1-min candles for both high and close to match backtest exactly."""
        now = datetime.now(IST)
        bar_min = getattr(self._strategy, 'candle_sl_minutes', 5)
        if now.minute % bar_min != 0:
            return
        check_key = f"{now.hour}:{now.minute}"
        if self.state.strategy_data.get("_candle_exit_check") == check_key:
            return
        self.state.strategy_data["_candle_exit_check"] = check_key

        with self._lock:
            pos = self.state.active_position
            if not pos:
                return
            sl_pts = self.state.settings.sl_points
            tgt_pts = self.state.settings.target_points
            token = self.state.instrument_token
            entry_time = pos.entry_time
            remaining_lots = pos.remaining_lots if pos.remaining_lots > 0 else self.state.settings.quantity

        # Fetch 1-min candles for high and close
        try:
            today = now.date()
            candles = self.client.kite.historical_data(
                token, today, today, "minute"
            )
        except Exception as e:
            logger.error(f"Candle exit check — failed to fetch candles: {e}")
            return

        if not candles:
            return

        high_since_entry = pos.entry_price
        for c in candles:
            cdt = c["date"]
            c_time = cdt.strftime("%H:%M:%S") if hasattr(cdt, 'strftime') else str(cdt)
            if c_time > entry_time:
                high_since_entry = max(high_since_entry, c["high"])

        bar_close = candles[-1]["close"]
        drop = high_since_entry - bar_close

        # Scale out: exit 1 group at each target crossed
        exit_per_target = self.state.settings.quantity // self._strategy.lot_multiplier
        if remaining_lots > exit_per_target and bar_close >= pos.target_price:
            groups_to_exit = 0
            while bar_close >= pos.target_price and remaining_lots - (groups_to_exit * exit_per_target) > exit_per_target:
                pos.target_price = self._round_tick(pos.target_price + tgt_pts)
                pos.sl_price = self._round_tick(pos.sl_price + sl_pts)
                groups_to_exit += 1

            lots_to_exit = groups_to_exit * exit_per_target
            if lots_to_exit > 0:
                exit_qty = lots_to_exit * self.state.lot_size
                with self._lock:
                    symbol = self.state.trading_symbol
                fill_price = self._manual_sl_exit(symbol, pos.direction, exit_qty)
                if fill_price is not None:
                    pnl = (fill_price - pos.entry_price) * lots_to_exit * self.state.lot_size
                    with self._lock:
                        pos.remaining_lots -= lots_to_exit
                        remaining_lots = pos.remaining_lots
                        self._save_state()
                    logger.info(
                        f"Scale out: exited {lots_to_exit} lot(s) @ {fill_price:.2f}, "
                        f"remaining={pos.remaining_lots}, P&L={pnl:.2f}"
                    )

        logger.info(
            f"Candle exit check: high={high_since_entry:.2f}, "
            f"bar_close={bar_close:.2f}, drop={drop:.2f}, sl={sl_pts}, "
            f"target={pos.target_price:.2f}, lots={remaining_lots}"
        )

        if drop > sl_pts:
            logger.info(
                f"Candle SL triggered: drop {drop:.2f} > {sl_pts}, "
                f"exiting all {remaining_lots} lot(s) at market"
            )
            self._reversal_in_progress = True
            try:
                with self._lock:
                    symbol = self.state.trading_symbol
                    direction = pos.direction
                    remaining_lots = pos.remaining_lots if pos.remaining_lots > 0 else self.state.settings.quantity
                    quantity = remaining_lots * self.state.lot_size

                fill_price = self._manual_sl_exit(symbol, direction, quantity)
                if fill_price is None:
                    logger.error("Candle SL — exit failed after 5 attempts. Will retry next check.")
                    return
                self._close_position(exit_price=fill_price)
                logger.info(f"Candle SL exit filled @ {fill_price}")

                ctx = self._make_context(current_direction=pos.direction)
                result = self._strategy.on_sl_hit(ctx, self.state.strategy_data)
                action = result.get("action", "stop")
                new_direction = result.get("direction", "")

                if action == "reselect_and_enter":
                    logger.info(f"Candle SL → reselecting and entering {new_direction}")
                    self._stop_ticker()
                    with self._lock:
                        self.state.trading_symbol = ""
                        self.state.instrument_token = 0
                        self.state.current_ltp = 0.0
                        self._save_state()
                    if self._select_instrument():
                        self._start_ticker()
                        time.sleep(2)
                        self._pending_direction = new_direction
                        self._enter_position(new_direction)
                elif action == "reverse":
                    logger.info(f"Candle SL → reversing to {new_direction}")
                    self._pending_direction = new_direction
                    self._enter_position(new_direction)
                else:
                    logger.info("Candle SL → strategy chose to stop")
                    self._stop_ticker()
                    self._running = False
                    with self._lock:
                        self.state.engine_status = "STOPPED"
                        self.state.status_message = "Stopped after candle SL"
                        self._save_state()
            finally:
                self._reversal_in_progress = False

    def _monitor_sl_fill_slots(self):
        """Dual-position mode: detect SL breach and manually exit for each slot."""
        for opt in ("CE", "PE"):
            with self._lock:
                slot = self.state.position_slots.get(opt)
                if not slot or not slot.active_position:
                    self._sl_breach_time.pop(opt, None)
                    continue
                pos = slot.active_position
                if not pos.sl_order_id:
                    self._sl_breach_time.pop(opt, None)
                    continue
                ltp = slot.current_ltp
                if ltp <= 0:
                    continue
                sl_price = pos.sl_price
                direction = pos.direction
                sl_order_id = pos.sl_order_id
                symbol = slot.trading_symbol
                qty = pos.remaining_lots * slot.lot_size

            sl_breached = False
            if direction == "BUY" and ltp <= sl_price:
                sl_breached = True
            elif direction == "SELL" and ltp >= sl_price:
                sl_breached = True

            if not sl_breached:
                self._sl_breach_time.pop(opt, None)
                continue

            now = time.time()
            if opt not in self._sl_breach_time:
                self._sl_breach_time[opt] = now
                logger.warning(f"[{opt}] SL breach detected: LTP={ltp}, SL={sl_price}. Monitoring...")
                continue

            if now - self._sl_breach_time[opt] < 3:
                continue

            self._sl_breach_time.pop(opt, None)

            # Re-check position still active
            with self._lock:
                slot = self.state.position_slots.get(opt)
                if not slot or not slot.active_position:
                    continue

            # Check order status
            try:
                history = self.client.kite.order_history(sl_order_id)
                latest = history[-1] if history else {}
                status = latest.get("status", "")
            except Exception as e:
                logger.error(f"[{opt}] SL status check failed: {e}")
                continue

            if status == "COMPLETE":
                continue

            logger.warning(
                f"[{opt}] SL not filled after breach (status={status}, LTP={ltp}, SL={sl_price}). "
                f"Cancelling SL and placing manual exit."
            )
            self._cancel_order(sl_order_id)

            fill_price = self._manual_sl_exit(symbol, direction, qty)
            if fill_price is None:
                logger.error(f"[{opt}] Manual SL exit failed — position still open!")
                continue

            logger.info(f"[{opt}] Manual SL exit filled @ {fill_price}")
            self._handle_sl_hit_slot(opt, fill_price)

    def _check_candle_exit_slots(self):
        """Candle-based SL for signal-based strategies (dual CE+PE slots).
        At N-min boundary, exit if (high_since_entry - bar_close) > sl_points.
        Uses 1-min candles for both high and close to match backtest exactly."""
        now = datetime.now(IST)
        bar_min = getattr(self._strategy, 'candle_sl_minutes', 5)
        if now.minute % bar_min != 0:
            return
        check_key = f"{now.hour}:{now.minute}"
        if self.state.strategy_data.get("_candle_exit_check_slots") == check_key:
            return
        self.state.strategy_data["_candle_exit_check_slots"] = check_key

        for opt in ("CE", "PE"):
            with self._lock:
                slot = self.state.position_slots.get(opt)
                if not slot or not slot.active_position:
                    continue
                pos = slot.active_position
                sl_pts = self.state.settings.sl_points
                token = slot.instrument_token
                entry_time = pos.entry_time
                symbol = slot.trading_symbol
                remaining_lots = pos.remaining_lots if pos.remaining_lots > 0 else self.state.settings.quantity

            # Fetch 1-min candles for high and close
            try:
                today = now.date()
                candles = self.client.kite.historical_data(
                    token, today, today, "minute"
                )
            except Exception as e:
                logger.error(f"[{opt}] Candle exit check — failed to fetch candles: {e}")
                continue

            if not candles:
                continue

            high_since_entry = pos.entry_price
            for c in candles:
                cdt = c["date"]
                c_time = cdt.strftime("%H:%M:%S") if hasattr(cdt, 'strftime') else str(cdt)
                if c_time > entry_time:
                    high_since_entry = max(high_since_entry, c["high"])

            bar_close = candles[-1]["close"]
            drop = high_since_entry - bar_close

            logger.info(
                f"[{opt}] Candle exit check: high={high_since_entry:.2f}, "
                f"bar_close={bar_close:.2f}, drop={drop:.2f}, sl={sl_pts}"
            )

            if drop > sl_pts:
                logger.info(
                    f"[{opt}] Candle SL triggered: drop {drop:.2f} > {sl_pts}, "
                    f"exiting at market"
                )
                qty = remaining_lots * slot.lot_size
                fill_price = self._manual_sl_exit(symbol, pos.direction, qty)
                if fill_price is None:
                    logger.error(f"[{opt}] Candle SL exit failed — will retry next check")
                    continue

                logger.info(f"[{opt}] Candle SL exit filled @ {fill_price}")
                self._handle_sl_hit_slot(opt, fill_price)

    # ── Position Entry / Exit / Reversal ────────────────────────────────

    def _enter_position(self, direction: str):
        """Enter a new position with SL protection."""
        if self._check_daily_cutoff():
            logger.info("Daily cutoff reached — skipping new entry")
            with self._lock:
                self.state.status_message = "Daily cutoff reached — no new entries"
                self._save_state()
            return

        ltp = self._get_ltp()
        sl_pts = self.state.settings.sl_points
        tgt_pts = self.state.settings.target_points

        # Clean up stale orders that may be blocking margin
        self._cancel_open_orders()

        # Retry entry with fresh LTP every 3 seconds (max 5 attempts)
        fill_price = None
        max_attempts = 5
        for attempt in range(1, max_attempts + 1):
            order_id = self._place_entry_order(direction)
            if not order_id:
                return

            fill_price = self._get_fill_price(order_id, timeout=3)
            if fill_price is not None:
                break

            # Not filled — cancel and retry with updated LTP
            self._cancel_order(order_id)
            if attempt < max_attempts:
                logger.info(
                    f"Entry not filled (attempt {attempt}/{max_attempts}), "
                    f"retrying with fresh LTP"
                )

        if fill_price is None:
            logger.error(f"Entry failed after {max_attempts} attempts")
            return

        if direction == "SELL":
            sl_price = self._round_tick(fill_price + sl_pts)
            target_price = self._round_tick(fill_price - tgt_pts)
        else:
            sl_price = self._round_tick(fill_price - sl_pts)
            target_price = self._round_tick(fill_price + tgt_pts)

        if getattr(self._strategy, 'use_exchange_sl', True):
            sl_order_id = self._place_sl_order(direction, sl_price)
            if not sl_order_id:
                logger.error("SL-M placement failed — closing entry immediately")
                close_dir = "BUY" if direction == "SELL" else "SELL"
                self._place_entry_order(close_dir)
                return
        else:
            sl_order_id = ""

        now_str = datetime.now(IST).strftime("%H:%M:%S")

        with self._lock:
            pos = ActivePosition(
                direction=direction,
                entry_price=fill_price,
                sl_price=sl_price,
                target_price=target_price,
                sl_order_id=sl_order_id,
                order_id=order_id,
                entry_time=now_str,
            )
            pos.remaining_lots = self.state.settings.quantity
            self.state.active_position = pos
            self._pending_partial_exits = 0
            self._save_state()

        logger.info(
            f"Position entered: {direction} @ {fill_price}, "
            f"SL={sl_price}, Target={target_price}"
        )

    def _close_position(self, exit_price: float = 0.0):
        """Record completed trade and clear active position."""
        with self._lock:
            pos = self.state.active_position
            if not pos:
                return

            now = datetime.now(IST)
            now_str = now.strftime("%H:%M:%S")

            remaining_lots = pos.remaining_lots if pos.remaining_lots > 0 else self.state.settings.quantity
            remaining_qty = remaining_lots * self.state.lot_size
            if pos.direction == "SELL":
                pnl = (pos.entry_price - exit_price) * remaining_qty
            else:
                pnl = (exit_price - pos.entry_price) * remaining_qty

            trade = TradeRecord(
                direction=pos.direction,
                entry_price=pos.entry_price,
                exit_price=exit_price,
                entry_time=pos.entry_time,
                exit_time=now_str,
                pnl=round(pnl, 2),
                quantity=remaining_lots,
                date=now.strftime("%Y-%m-%d"),
                symbol=self.state.trading_symbol,
            )
            self.state.trades_today.append(trade)
            self.state.total_pnl = round(self.state.total_pnl + pnl, 2)
            self.state.active_position = None
            self._save_state()

        # Persist to all-time history (append-only JSONL)
        self._append_trade_history(trade)

        logger.info(
            f"Trade closed: {trade.direction} "
            f"entry={trade.entry_price} exit={trade.exit_price} P&L={trade.pnl}"
        )

    def _append_trade_history(self, trade: TradeRecord):
        """Append a single trade to the persistent JSONL history file."""
        try:
            with open(HISTORY_FILE, "a") as f:
                f.write(json.dumps(asdict(trade)) + "\n")
        except Exception as e:
            logger.error(f"Failed to write trade history: {e}")

    @staticmethod
    def load_trade_history() -> list[dict]:
        """Load all-time trade history from JSONL file."""
        trades = []
        if not HISTORY_FILE.exists():
            return trades
        try:
            for line in HISTORY_FILE.read_text().splitlines():
                line = line.strip()
                if line:
                    trades.append(json.loads(line))
        except Exception as e:
            logger.error(f"Failed to read trade history: {e}")
        return trades

    def _check_daily_cutoff(self) -> bool:
        """Check if daily P&L exceeds cutoff thresholds. Returns True if cutoff hit."""
        s = self.state.settings
        if not s.daily_cutoff:
            return False
        total_pnl = self.state.total_pnl
        notional = s.quantity * self.state.lot_size * s.min_premium
        if notional <= 0:
            return False
        profit_limit = notional * s.daily_profit_pct / 100
        loss_limit = notional * s.daily_loss_pct / 100
        if total_pnl >= profit_limit:
            logger.info(f"Daily PROFIT cutoff hit: P&L {total_pnl:.2f} >= {profit_limit:.2f}")
            return True
        if total_pnl <= -loss_limit:
            logger.info(f"Daily LOSS cutoff hit: P&L {total_pnl:.2f} <= -{loss_limit:.2f}")
            return True
        return False

    def _handle_sl_hit(self, sl_fill_price: float):
        """SL hit → close trade, delegate next action to the strategy."""
        with self._lock:
            pos = self.state.active_position
            if not pos:
                return
            old_direction = pos.direction

        self._close_position(exit_price=sl_fill_price)

        ctx = self._make_context(current_direction=old_direction)
        result = self._strategy.on_sl_hit(ctx, self.state.strategy_data)
        action = result.get("action", "stop")
        new_direction = result.get("direction", "")

        if action == "reverse":
            logger.info(f"Reversing: {old_direction} → {new_direction}")
            self._pending_direction = new_direction
            self._enter_position(new_direction)

        elif action == "reselect_and_enter":
            logger.info(f"Reselecting instrument and entering {new_direction}")
            self._stop_ticker()
            with self._lock:
                self.state.trading_symbol = ""
                self.state.instrument_token = 0
                self.state.current_ltp = 0.0
                self._save_state()
            if not self._select_instrument():
                logger.error("Instrument re-selection failed after SL hit")
                return
            self._start_ticker()
            time.sleep(2)
            self._pending_direction = new_direction
            self._enter_position(new_direction)

        else:  # "stop"
            logger.info("Strategy chose to stop after SL hit")
            self._stop_ticker()
            self._running = False
            with self._lock:
                self.state.engine_status = "STOPPED"
                self.state.status_message = "Stopped after SL hit"
                self._save_state()

    # Extra buffer added to limit price when square-off needs to be aggressive
    SQUARE_OFF_BUFFER = 20.0

    def _square_off(self):
        """Cancel SL and close position with aggressive LIMIT order.

        Uses a wider price buffer than normal entries to maximise fill
        probability.  If the first attempt doesn't fill, retries once
        with an even wider buffer.  **Never** records a fake exit at 0.0 —
        if both attempts fail the position stays tracked so the user can
        handle it manually.
        """
        with self._lock:
            pos = self.state.active_position
            if not pos:
                return
            sl_order_id = pos.sl_order_id
            direction = pos.direction

        if sl_order_id:
            self._cancel_order(sl_order_id)

        with self._lock:
            remaining_lots = pos.remaining_lots if pos.remaining_lots > 0 else self.state.settings.quantity
            remaining_qty = remaining_lots * self.state.lot_size

        close_dir = "BUY" if direction == "SELL" else "SELL"

        # Attempt 1: normal entry order (LIMIT with proportional buffer)
        order_id = self._place_entry_order(close_dir, quantity=remaining_qty)
        if order_id:
            fill_price = self._get_fill_price(order_id, timeout=30, cancel_on_timeout=True)
            if fill_price is not None:
                self._close_position(exit_price=fill_price)
                return

        # Attempt 2: aggressive LIMIT with wider buffer
        logger.warning("Square-off attempt 1 failed — retrying with wider buffer")
        ltp = self._get_ltp()
        if ltp > 0:
            if close_dir == "BUY":
                price = self._round_tick(ltp + self.SQUARE_OFF_BUFFER)
            else:
                price = self._round_tick(ltp - self.SQUARE_OFF_BUFFER)
            try:
                order_id2 = self.client.kite.place_order(
                    variety="regular", exchange="NFO",
                    tradingsymbol=self.state.trading_symbol,
                    transaction_type=close_dir, quantity=remaining_qty,
                    product=self.state.settings.product,
                    order_type="LIMIT", price=price,
                )
                order_id2 = str(order_id2)
                logger.info(f"Aggressive square-off: {close_dir} LIMIT@{price}, order_id={order_id2}")
                fill_price = self._get_fill_price(order_id2, timeout=60, cancel_on_timeout=True)
                if fill_price is not None:
                    self._close_position(exit_price=fill_price)
                    return
            except Exception as e:
                logger.error(f"Aggressive square-off order failed: {e}")

        # Both attempts failed — do NOT fake-close
        logger.error(
            "SQUARE-OFF FAILED: position still open on exchange! "
            "Manual intervention required."
        )

    # ── Tick Handling (Trailing Logic) ──────────────────────────────────

    def _handle_tick(self, ltp: float):
        """Check if target hit → trail SL / trigger partial exits. Called from ticker thread."""
        if not self._tick_ready:
            return
        if not getattr(self._strategy, 'use_targets', True):
            with self._lock:
                self.state.current_ltp = ltp
            return

        sl_order_id = None
        new_trigger = None
        targets_crossed = 0
        partial_exits = 0

        with self._lock:
            self.state.current_ltp = ltp
            pos = self.state.active_position
            if not pos or not pos.entry_price:
                return

            sl_pts = self.state.settings.sl_points
            tgt_pts = self.state.settings.target_points

            if pos.direction == "SELL":
                while ltp <= pos.target_price:
                    pos.target_price = self._round_tick(pos.target_price - tgt_pts)
                    pos.sl_price = self._round_tick(pos.sl_price - sl_pts)
                    targets_crossed += 1
            elif pos.direction == "BUY":
                while ltp >= pos.target_price:
                    pos.target_price = self._round_tick(pos.target_price + tgt_pts)
                    pos.sl_price = self._round_tick(pos.sl_price + sl_pts)
                    targets_crossed += 1

            if targets_crossed > 0:
                # Recovery mode: trail SL only, no partial exits
                if not self._recovery_mode:
                    for _ in range(targets_crossed):
                        result = self._strategy.on_target_hit(
                            self._make_context(pos.direction),
                            self.state.strategy_data,
                            pos.remaining_lots,
                        )
                        if result.get("action") == "partial_exit" and pos.remaining_lots > 1:
                            exit_n = min(result.get("exit_lots", 1), pos.remaining_lots - 1)
                            pos.remaining_lots -= exit_n
                            partial_exits += exit_n
                        # else: trail only (default)
                sl_order_id = pos.sl_order_id
                new_trigger = pos.sl_price
                self._pending_partial_exits += partial_exits
                self._save_state()

        # Outside lock: modify SL only if no partial exits pending
        # (partial exits handle SL replacement in _execute_partial_exit)
        if targets_crossed > 0 and partial_exits == 0 and sl_order_id:
            self._modify_sl_order(sl_order_id, new_trigger)
            logger.info(f"Trailed SL → {new_trigger}, target → {pos.target_price}")

    # ── Partial Exit Handling ─────────────────────────────────────────────

    def _execute_partial_exit(self):
        """Execute one pending partial exit. Called from engine loop."""
        with self._lock:
            pos = self.state.active_position
            if not pos or self._pending_partial_exits <= 0:
                return
            self._pending_partial_exits -= 1
            direction = pos.direction
            entry_price = pos.entry_price
            sl_order_id = pos.sl_order_id
            new_trigger = pos.sl_price
            remaining_qty = pos.remaining_lots * self.state.lot_size
            lot_size = self.state.lot_size

        # 1. Cancel old SL first — frees up the pending sell/buy quantity
        #    so the partial exit order won't be rejected for exceeding position
        self._cancel_order(sl_order_id)

        # 2. Place partial exit order (1 lot)
        close_dir = "BUY" if direction == "SELL" else "SELL"
        order_id = self._place_entry_order(close_dir, quantity=lot_size)
        if not order_id:
            logger.error("Partial exit order failed — re-placing SL for full qty")
            new_sl_id = self._place_sl_order(direction, new_trigger,
                                             quantity=remaining_qty + lot_size)
            with self._lock:
                if self.state.active_position and new_sl_id:
                    self.state.active_position.sl_order_id = new_sl_id
                    self.state.active_position.remaining_lots += 1
                    self._save_state()
            return

        fill_price = self._get_fill_price(order_id, timeout=60, cancel_on_timeout=True)
        if fill_price is None:
            logger.error("Partial exit fill timeout — re-placing SL")
            new_sl_id = self._place_sl_order(direction, new_trigger,
                                             quantity=remaining_qty)
            with self._lock:
                if self.state.active_position and new_sl_id:
                    self.state.active_position.sl_order_id = new_sl_id
                    self._save_state()
            return

        # 3. Record the partial trade
        self._record_partial_trade(direction, entry_price, fill_price, lot_size)

        # 4. Place new SL with reduced qty + trailed trigger
        new_sl_id = self._place_sl_order(direction, new_trigger, quantity=remaining_qty)

        with self._lock:
            if self.state.active_position and new_sl_id:
                self.state.active_position.sl_order_id = new_sl_id
                self._save_state()

        logger.info(
            f"Partial exit: {close_dir} 1 lot @ {fill_price}, "
            f"{pos.remaining_lots} lots remain, SL → {new_trigger}"
        )

    def _record_partial_trade(self, direction, entry_price, exit_price, actual_qty):
        """Record P&L for a partially exited lot without clearing the position."""
        now = datetime.now(IST)
        if direction == "SELL":
            pnl = (entry_price - exit_price) * actual_qty
        else:
            pnl = (exit_price - entry_price) * actual_qty
        trade = TradeRecord(
            direction=direction, entry_price=entry_price,
            exit_price=exit_price, entry_time="partial",
            exit_time=now.strftime("%H:%M:%S"),
            pnl=round(pnl, 2), quantity=1,
            date=now.strftime("%Y-%m-%d"),
            symbol=self.state.trading_symbol,
        )
        with self._lock:
            self.state.trades_today.append(trade)
            self.state.total_pnl = round(self.state.total_pnl + pnl, 2)
            self._save_state()
        self._append_trade_history(trade)

    # ── Order Update Handling ───────────────────────────────────────────

    def _handle_order_update(self, data: dict):
        """Detect SL-M fill → trigger reversal. Called from ticker thread."""
        order_id = str(data.get("order_id", ""))
        status = data.get("status", "")

        with self._lock:
            pos = self.state.active_position
            if not pos or pos.sl_order_id != order_id:
                return

        if status == "COMPLETE":
            fill_price = float(data.get("average_price", 0))
            logger.info(f"SL-M filled: order_id={order_id}, price={fill_price}")
            self._reversal_in_progress = True
            try:
                self._handle_sl_hit(fill_price)
            finally:
                self._reversal_in_progress = False

    # ── WebSocket Ticker ────────────────────────────────────────────────

    def _connect_ticker(self, ticker):
        """Connect a KiteTicker, handling Twisted reactor thread-safety.

        The Twisted reactor is a process-wide singleton that can only be
        started once.  The first connect() starts reactor.run() in a daemon
        thread.  Subsequent connects MUST schedule connectWS on that reactor
        thread via callFromThread — calling it from another thread silently
        fails.
        """
        from twisted.internet import reactor as _reactor

        if _reactor.running:
            # Reactor already alive from a previous connection.
            # Re-create factory + schedule connectWS on the reactor thread.
            from twisted.internet import ssl as _ssl
            from autobahn.twisted.websocket import connectWS as _connectWS

            ticker._create_connection(
                ticker.socket_url,
                useragent=ticker._user_agent(),
                headers={"X-Kite-Version": "3"},
            )
            ctx = _ssl.ClientContextFactory() if ticker.factory.isSecure else None
            _reactor.callFromThread(
                _connectWS, ticker.factory,
                contextFactory=ctx, timeout=ticker.connect_timeout,
            )
            logger.info("Ticker reconnect scheduled on existing reactor thread")
        else:
            # First connection ever — connect() starts reactor in a new thread.
            ticker.connect(threaded=True)
            logger.info("Ticker started with new reactor thread")

    def _start_ticker(self):
        if not self.client.is_authenticated:
            logger.error("Cannot start ticker: not authenticated")
            return

        token = self.state.instrument_token
        if not token:
            logger.error("Cannot start ticker: no instrument token")
            return

        # Reset cached ticker so we get a fresh WebSocket
        self.client._ticker = None
        self._tick_ready = False
        self._ticker = self.client.get_ticker()

        def on_ticks(ws, ticks):
            for tick in ticks:
                if tick["instrument_token"] == token:
                    self._handle_tick(tick["last_price"])

        def on_connect(ws, response):
            ws.subscribe([token])
            ws.set_mode(ws.MODE_LTP, [token])
            self._tick_ready = True
            logger.info(f"Ticker connected, subscribed to token {token}")

        def on_close(ws, code, reason):
            logger.warning(f"Ticker closed: code={code} reason={reason}")

        def on_error(ws, code, reason):
            logger.error(f"Ticker error: code={code} reason={reason}")

        def on_order_update(ws, data):
            self._handle_order_update(data)

        self._ticker.on_ticks = on_ticks
        self._ticker.on_connect = on_connect
        self._ticker.on_close = on_close
        self._ticker.on_error = on_error
        self._ticker.on_order_update = on_order_update

        self._connect_ticker(self._ticker)

    def _stop_ticker(self):
        if self._ticker:
            try:
                self._ticker.close()
            except Exception:
                pass
            self._ticker = None
            self.client._ticker = None

    # ── Crash Recovery ──────────────────────────────────────────────────

    def _attempt_recovery(self) -> bool:
        """Verify SL order status on restart and resume if still open."""
        with self._lock:
            pos = self.state.active_position
            if not pos:
                return False
            sl_order_id = pos.sl_order_id

        logger.info(
            f"Recovering: {pos.direction} @ {pos.entry_price}, "
            f"SL order: {sl_order_id}"
        )

        try:
            history = self.client.kite.order_history(sl_order_id)
            latest = history[-1] if history else {}
            status = latest.get("status", "")

            if status == "COMPLETE":
                fill_price = float(latest.get("average_price", 0))
                logger.info(f"SL filled during downtime @ {fill_price}")
                self._close_position(exit_price=fill_price)
                return False

            if status in ("CANCELLED", "REJECTED"):
                logger.warning(f"SL order was {status} — squaring off")
                self._square_off()
                return False

            logger.info(f"SL order still open ({status}) — resuming")
            return True

        except Exception as e:
            logger.error(f"Recovery check failed: {e}")
            return False

    # ── Main Engine Loop ────────────────────────────────────────────────

    def _engine_loop(self):
        """Background thread: wait → select instrument → trade → EOD close."""
        logger.info("Engine loop started")

        with self._lock:
            self.state.engine_status = "WAITING"
            self.state.status_message = "Starting..."
            self._save_state()

        entry_failures = 0

        while self._running:
            now = datetime.now(IST)
            current_time = now.strftime("%H:%M")

            # ── Past stop time → square off and exit ──
            if current_time >= self.state.settings.stop_time:
                if self._strategy.signal_based:
                    self._square_off_all_slots()
                elif self.state.active_position:
                    logger.info("Stop time reached — squaring off")
                    self._square_off()
                self._stop_ticker()
                with self._lock:
                    self.state.engine_status = "MARKET_CLOSED"
                    self.state.status_message = "Market closed"
                    self._save_state()
                break

            # ── Before start time → wait ──
            if current_time < self.state.settings.start_time:
                with self._lock:
                    if self.state.engine_status != "WAITING":
                        self.state.engine_status = "WAITING"
                        self.state.status_message = "Waiting for market open"
                        self._save_state()
                time.sleep(5)
                continue

            # ── Trading hours ──
            if not self.client.is_authenticated:
                logger.error("Not authenticated — stopping engine")
                with self._lock:
                    self.state.engine_status = "STOPPED"
                    self.state.status_message = "Not authenticated"
                    self._save_state()
                break

            if self.state.engine_status != "ACTIVE":
                with self._lock:
                    self.state.engine_status = "ACTIVE"
                    sym = self.state.trading_symbol
                    self.state.status_message = f"Trading {sym}" if sym else "Active"
                    self._save_state()

            # ── Signal-based mode (dual CE + PE positions) ──
            if self._strategy.signal_based:
                self._signal_based_loop_iteration()
                time.sleep(2)
                continue

            # ── Single-position mode (existing strategies) ──

            # Initialise strategy_data before first instrument selection
            if not self.state.trading_symbol and not self._pending_direction:
                self._strategy.initial_direction(self.state.strategy_data,
                                                  self.state.settings.market_bias)

            # Select instrument once per day
            if not self.state.trading_symbol:
                if not self._select_instrument():
                    logger.error("Instrument selection failed — stopping")
                    with self._lock:
                        self.state.engine_status = "STOPPED"
                        self.state.status_message = "Instrument selection failed"
                        self._save_state()
                    break

            # Update status message with symbol once we have it
            if self.state.trading_symbol:
                with self._lock:
                    self.state.status_message = f"Trading {self.state.trading_symbol}"

            # Start ticker once
            if not self._ticker:
                self._start_ticker()
                time.sleep(2)

            # Recovery mode: just monitor, wait for SL hit via order update
            if self._recovery_mode:
                if not self.state.active_position:
                    # Position was closed (SL hit detected via order update)
                    self._recovery_mode = False
                    logger.info("Recovery mode ended — position closed, resuming normal operation")
                time.sleep(2)
                continue

            # Execute pending partial exits
            if self._pending_partial_exits > 0:
                self._execute_partial_exit()
                continue  # re-check immediately

            # Enter position if none (initial SELL or retry after failed reversal)
            if not self.state.active_position and not self._reversal_in_progress:
                # Daily cutoff — stop entering new trades if P&L threshold exceeded
                if self._check_daily_cutoff():
                    with self._lock:
                        self.state.status_message = "Daily cutoff reached — no new entries"
                        self._save_state()
                    time.sleep(10)
                    continue

                if entry_failures >= 5:
                    logger.error("5 consecutive entry failures — stopping engine")
                    with self._lock:
                        self.state.engine_status = "STOPPED"
                        self.state.status_message = "Entry failed 5 times"
                        self._save_state()
                    break

                # Before each new entry, check premium and reselect if needed
                min_prem = max(20.0, self.state.settings.min_premium)
                ltp = self._get_ltp()
                if self.state.trading_symbol and ltp > 0 and ltp < min_prem:
                    if entry_failures >= 1:
                        # Already reselected once — accept whatever we have
                        logger.info(
                            f"No option with premium >= {min_prem}, "
                            f"proceeding with {self.state.trading_symbol} (premium: {ltp:.1f})"
                        )
                    else:
                        logger.warning(
                            f"Premium {ltp:.1f} < min {min_prem} on "
                            f"{self.state.trading_symbol}, reselecting instrument"
                        )
                        self._stop_ticker()
                        with self._lock:
                            self.state.trading_symbol = ""
                            self.state.instrument_token = 0
                            self.state.current_ltp = 0.0
                            self._save_state()
                        entry_failures += 1
                        time.sleep(2)
                        continue

                if self._pending_direction:
                    direction = self._pending_direction
                else:
                    direction = self._strategy.initial_direction(
                        self.state.strategy_data, self.state.settings.market_bias)
                self._pending_direction = None
                self._enter_position(direction)
                if not self.state.active_position:
                    entry_failures += 1
                    logger.warning(f"Entry attempt {entry_failures}/5 failed, "
                                   f"retrying in 10s")
                    time.sleep(10)
                    continue
                entry_failures = 0

            # Monitor SL fill — detect and handle unfilled SL in fast markets
            if self.state.active_position and not self._reversal_in_progress:
                if getattr(self._strategy, 'use_exchange_sl', True):
                    self._monitor_sl_fill()
                elif self._strategy.lot_multiplier > 1:
                    self._check_candle_exit_scaleout()
                else:
                    self._check_candle_exit_alt()

            time.sleep(2)

        self._running = False
        logger.info("Engine loop exited")

    # ── Signal-Based (Dual-Position) Engine ─────────────────────────────

    def _signal_based_loop_iteration(self):
        """One iteration of the engine loop for signal-based strategies."""
        # 1. Pre-select both instruments if not done
        if "_ce_token" not in self.state.strategy_data:
            self._preselect_both_options()
            if "_ce_token" not in self.state.strategy_data:
                logger.error("Dual instrument selection failed — stopping")
                with self._lock:
                    self.state.engine_status = "STOPPED"
                    self.state.status_message = "Instrument selection failed"
                    self._save_state()
                self._running = False
                return
            self._start_dual_ticker()
            return

        # 2. Handle pending partial exits for each slot
        for opt in ("CE", "PE"):
            slot = self.state.position_slots.get(opt)
            if slot and slot.pending_partial_exits > 0:
                self._execute_partial_exit_slot(opt)
                return

        # 2b. Monitor SL fill — detect and handle unfilled SL in fast markets
        if getattr(self._strategy, 'use_exchange_sl', True):
            self._monitor_sl_fill_slots()
        else:
            self._check_candle_exit_slots()

        # 3. Copy live LTPs into strategy_data for signal check
        for opt in ("CE", "PE"):
            slot = self.state.position_slots.get(opt)
            if slot:
                self.state.strategy_data[f"_ltp_{opt}"] = slot.current_ltp

        # 3b. Poll for entry signals (every iteration, ~2s)
        # Daily cutoff — skip new entries if P&L threshold exceeded
        if self._check_daily_cutoff():
            with self._lock:
                self.state.status_message = "Daily cutoff reached — no new entries"
                self._save_state()
            return

        signals = self._strategy.get_entry_signal(
            self.client, self.state.settings, self.state.strategy_data,
        )
        if signals:
            for sig in signals:
                opt = sig["option_type"]
                slot = self.state.position_slots.get(opt)
                if slot and not slot.active_position:
                    self._enter_position_slot(opt, sig["direction"])

        # 4. Update status
        parts = []
        for opt in ("CE", "PE"):
            slot = self.state.position_slots.get(opt)
            if slot and slot.active_position:
                parts.append(f"{opt} active ({slot.trading_symbol})")
        if not parts:
            parts.append("Waiting for signal")
        with self._lock:
            self.state.status_message = " | ".join(parts)

    def _preselect_both_options(self):
        """Select CE and PE instruments, store in strategy_data and position_slots."""
        from .base_strategy import select_nifty_option

        min_prem = self.state.settings.min_premium
        exp_type = self.state.settings.expiry_type

        ce_inst = select_nifty_option(self.client, "CE", min_prem, exp_type)
        pe_inst = select_nifty_option(self.client, "PE", min_prem, exp_type)

        if not ce_inst or not pe_inst:
            logger.error("Failed to select CE and/or PE instruments")
            return

        # Enforce quantity is a multiple of lot_multiplier
        min_lots = self._strategy.lot_multiplier
        with self._lock:
            if self.state.settings.quantity < min_lots or self.state.settings.quantity % min_lots != 0:
                self.state.settings.quantity = min_lots

        ce_lot = int(ce_inst.get("lot_size", 1))
        pe_lot = int(pe_inst.get("lot_size", 1))

        with self._lock:
            self.state.strategy_data["_ce_token"] = ce_inst["instrument_token"]
            self.state.strategy_data["_ce_symbol"] = ce_inst["tradingsymbol"]
            self.state.strategy_data["_ce_lot_size"] = ce_lot
            self.state.strategy_data["_pe_token"] = pe_inst["instrument_token"]
            self.state.strategy_data["_pe_symbol"] = pe_inst["tradingsymbol"]
            self.state.strategy_data["_pe_lot_size"] = pe_lot

            self.state.position_slots["CE"] = PositionSlot(
                option_type="CE",
                instrument_token=ce_inst["instrument_token"],
                trading_symbol=ce_inst["tradingsymbol"],
                lot_size=ce_lot,
            )
            self.state.position_slots["PE"] = PositionSlot(
                option_type="PE",
                instrument_token=pe_inst["instrument_token"],
                trading_symbol=pe_inst["tradingsymbol"],
                lot_size=pe_lot,
            )
            self._save_state()

        logger.info(
            f"Pre-selected CE: {ce_inst['tradingsymbol']} (token={ce_inst['instrument_token']}), "
            f"PE: {pe_inst['tradingsymbol']} (token={pe_inst['instrument_token']})"
        )

    def _start_dual_ticker(self):
        """Subscribe to both CE and PE tokens for real-time LTP updates."""
        if not self.client.is_authenticated:
            logger.error("Cannot start dual ticker: not authenticated")
            return

        ce_token = self.state.strategy_data.get("_ce_token")
        pe_token = self.state.strategy_data.get("_pe_token")
        if not ce_token or not pe_token:
            logger.error("Cannot start dual ticker: missing tokens")
            return

        tokens = [ce_token, pe_token]

        # Reset cached ticker so we get a fresh WebSocket
        self.client._ticker = None
        self._tick_ready = False
        self._ticker = self.client.get_ticker()

        def on_ticks(ws, ticks):
            for tick in ticks:
                tok = tick["instrument_token"]
                ltp = tick["last_price"]
                for opt in ("CE", "PE"):
                    slot = self.state.position_slots.get(opt)
                    if slot and slot.instrument_token == tok:
                        slot.current_ltp = ltp
                        if slot.active_position:
                            self._handle_tick_slot(opt, ltp)
                        break

        def on_connect(ws, response):
            ws.subscribe(tokens)
            ws.set_mode(ws.MODE_LTP, tokens)
            self._tick_ready = True
            logger.info(f"Dual ticker connected, subscribed to tokens {tokens}")

        def on_close(ws, code, reason):
            logger.warning(f"Dual ticker closed: code={code} reason={reason}")

        def on_error(ws, code, reason):
            logger.error(f"Dual ticker error: code={code} reason={reason}")

        def on_order_update(ws, data):
            self._handle_order_update_dual(data)

        self._ticker.on_ticks = on_ticks
        self._ticker.on_connect = on_connect
        self._ticker.on_close = on_close
        self._ticker.on_error = on_error
        self._ticker.on_order_update = on_order_update

        self._connect_ticker(self._ticker)

    def _handle_tick_slot(self, opt_type: str, ltp: float):
        """Check target/trail for a specific position slot. Called from ticker thread."""
        if not self._tick_ready:
            return
        if not getattr(self._strategy, 'use_targets', True):
            return

        sl_order_id = None
        new_trigger = None
        targets_crossed = 0
        partial_exits = 0

        with self._lock:
            slot = self.state.position_slots.get(opt_type)
            if not slot or not slot.active_position:
                return

            pos = slot.active_position
            sl_pts = self.state.settings.sl_points
            tgt_pts = self.state.settings.target_points

            if pos.direction == "BUY":
                while ltp >= pos.target_price:
                    pos.target_price = self._round_tick(pos.target_price + tgt_pts)
                    pos.sl_price = self._round_tick(pos.sl_price + sl_pts)
                    targets_crossed += 1

            if targets_crossed > 0:
                for _ in range(targets_crossed):
                    ctx = StrategyContext(
                        client=self.client, settings=self.state.settings,
                        trading_symbol=slot.trading_symbol,
                        current_direction=pos.direction,
                    )
                    result = self._strategy.on_target_hit(
                        ctx, self.state.strategy_data, pos.remaining_lots,
                    )
                    if result.get("action") == "partial_exit" and pos.remaining_lots > 1:
                        exit_n = min(result.get("exit_lots", 1), pos.remaining_lots - 1)
                        pos.remaining_lots -= exit_n
                        partial_exits += exit_n

                sl_order_id = pos.sl_order_id
                new_trigger = pos.sl_price
                slot.pending_partial_exits += partial_exits
                self._save_state()

        if targets_crossed > 0 and partial_exits == 0 and sl_order_id:
            self._modify_sl_order_slot(opt_type, sl_order_id, new_trigger)
            logger.info(f"[{opt_type}] Trailed SL -> {new_trigger}")

    def _handle_order_update_dual(self, data: dict):
        """Detect SL fill for signal-based strategies. Match to correct slot."""
        order_id = str(data.get("order_id", ""))
        status = data.get("status", "")

        if status != "COMPLETE":
            return

        with self._lock:
            for opt in ("CE", "PE"):
                slot = self.state.position_slots.get(opt)
                if slot and slot.active_position and slot.active_position.sl_order_id == order_id:
                    fill_price = float(data.get("average_price", 0))
                    logger.info(f"[{opt}] SL filled: order_id={order_id}, price={fill_price}")
                    break
            else:
                return

        # Handle SL hit for this slot (outside lock)
        self._handle_sl_hit_slot(opt, fill_price)

    def _handle_sl_hit_slot(self, opt_type: str, sl_fill_price: float):
        """SL hit for a signal-based slot. Close trade, delegate to strategy."""
        self._close_position_slot(opt_type, exit_price=sl_fill_price)

        self.state.strategy_data["_sl_hit_option"] = opt_type
        ctx = StrategyContext(
            client=self.client, settings=self.state.settings,
            trading_symbol=self.state.position_slots[opt_type].trading_symbol,
            current_direction="BUY",
        )
        self._strategy.on_sl_hit(ctx, self.state.strategy_data)
        logger.info(f"[{opt_type}] SL hit — waiting for next entry signal")

    def _enter_position_slot(self, opt_type: str, direction: str):
        """Enter a new position for a specific CE/PE slot."""
        if self._check_daily_cutoff():
            logger.info(f"[{opt_type}] Daily cutoff reached — skipping new entry")
            with self._lock:
                self.state.status_message = "Daily cutoff reached — no new entries"
                self._save_state()
            return

        slot = self.state.position_slots.get(opt_type)
        if not slot:
            return

        ltp = slot.current_ltp
        if ltp <= 0:
            # Try REST API fallback
            try:
                sym = f"NFO:{slot.trading_symbol}"
                data = self.client.kite.ltp(sym)
                ltp = data[sym]["last_price"]
            except Exception:
                logger.error(f"[{opt_type}] Cannot enter: no LTP available")
                return

        # min_premium is for initial instrument selection only.
        # Once instrument is selected, only enforce a basic floor.
        if ltp < 20.0:
            logger.warning(f"[{opt_type}] Skipping entry: premium {ltp:.1f} < floor 20")
            return

        # Cancel stale orders for this symbol
        self._cancel_open_orders_for(slot.trading_symbol)

        qty = self.state.settings.quantity * slot.lot_size

        # Retry entry with fresh LTP every 3 seconds (max 5 attempts)
        fill_price = None
        max_attempts = 5
        for attempt in range(1, max_attempts + 1):
            ltp = slot.current_ltp
            if ltp <= 0:
                try:
                    sym = f"NFO:{slot.trading_symbol}"
                    data = self.client.kite.ltp(sym)
                    ltp = data[sym]["last_price"]
                except Exception:
                    pass
            if ltp <= 0:
                logger.error(f"[{opt_type}] Cannot enter: no LTP available")
                return

            buf = self._entry_buffer(ltp)
            price = self._round_tick(ltp + buf) if direction == "BUY" else self._round_tick(ltp - buf)

            try:
                order_id = self.client.kite.place_order(
                    variety="regular", exchange="NFO",
                    tradingsymbol=slot.trading_symbol,
                    transaction_type=direction, quantity=qty,
                    product=self.state.settings.product,
                    order_type="LIMIT", price=price,
                )
                order_id = str(order_id)
                logger.info(f"[{opt_type}] Entry order: {direction} {slot.trading_symbol} LIMIT@{price}, order_id={order_id}")
            except Exception as e:
                logger.error(f"[{opt_type}] Entry order failed: {e}")
                return

            fill_price = self._get_fill_price(order_id, timeout=3)
            if fill_price is not None:
                break

            self._cancel_order(order_id)
            if attempt < max_attempts:
                logger.info(f"[{opt_type}] Entry not filled (attempt {attempt}/{max_attempts}), retrying with fresh LTP")

        if fill_price is None:
            logger.error(f"[{opt_type}] Entry failed after {max_attempts} attempts")
            return

        sl_pts = self.state.settings.sl_points
        tgt_pts = self.state.settings.target_points
        sl_price = self._round_tick(fill_price - sl_pts) if direction == "BUY" else self._round_tick(fill_price + sl_pts)
        target_price = self._round_tick(fill_price + tgt_pts) if direction == "BUY" else self._round_tick(fill_price - tgt_pts)

        if getattr(self._strategy, 'use_exchange_sl', True):
            sl_order_id = self._place_sl_order_for(
                slot.trading_symbol, direction, sl_price, qty,
            )
            if not sl_order_id:
                logger.error(f"[{opt_type}] SL placement failed — closing entry immediately")
                close_dir = "SELL" if direction == "BUY" else "BUY"
                try:
                    self.client.kite.place_order(
                        variety="regular", exchange="NFO",
                        tradingsymbol=slot.trading_symbol,
                        transaction_type=close_dir, quantity=qty,
                        product=self.state.settings.product,
                        order_type="LIMIT",
                        price=self._round_tick(ltp - self._entry_buffer(ltp) if close_dir == "SELL" else ltp + self._entry_buffer(ltp)),
                    )
                except Exception:
                    pass
                return
        else:
            sl_order_id = ""

        now_str = datetime.now(IST).strftime("%H:%M:%S")

        with self._lock:
            slot.active_position = ActivePosition(
                direction=direction,
                entry_price=fill_price,
                sl_price=sl_price,
                target_price=target_price,
                sl_order_id=sl_order_id,
                order_id=order_id,
                entry_time=now_str,
                remaining_lots=self.state.settings.quantity,
            )
            self.state.strategy_data[f"_active_{opt_type}"] = True
            self._save_state()

        logger.info(
            f"[{opt_type}] Position entered: {direction} @ {fill_price}, "
            f"SL={sl_price}, Target={target_price}"
        )

    def _close_position_slot(self, opt_type: str, exit_price: float = 0.0):
        """Record completed trade for a specific slot and clear its position."""
        with self._lock:
            slot = self.state.position_slots.get(opt_type)
            if not slot or not slot.active_position:
                return

            pos = slot.active_position
            now = datetime.now(IST)
            now_str = now.strftime("%H:%M:%S")

            remaining_lots = pos.remaining_lots if pos.remaining_lots > 0 else self.state.settings.quantity
            remaining_qty = remaining_lots * slot.lot_size
            pnl = (exit_price - pos.entry_price) * remaining_qty if pos.direction == "BUY" else (pos.entry_price - exit_price) * remaining_qty

            trade = TradeRecord(
                direction=pos.direction,
                entry_price=pos.entry_price,
                exit_price=exit_price,
                entry_time=pos.entry_time,
                exit_time=now_str,
                pnl=round(pnl, 2),
                quantity=remaining_lots,
                date=now.strftime("%Y-%m-%d"),
                symbol=slot.trading_symbol,
            )
            self.state.trades_today.append(trade)
            self.state.total_pnl = round(self.state.total_pnl + pnl, 2)
            slot.active_position = None
            slot.pending_partial_exits = 0
            self.state.strategy_data[f"_active_{opt_type}"] = False
            self._save_state()

        self._append_trade_history(trade)
        logger.info(
            f"[{opt_type}] Trade closed: {trade.direction} "
            f"entry={trade.entry_price} exit={trade.exit_price} P&L={trade.pnl}"
        )

    def _execute_partial_exit_slot(self, opt_type: str):
        """Execute one pending partial exit for a specific slot."""
        with self._lock:
            slot = self.state.position_slots.get(opt_type)
            if not slot or not slot.active_position or slot.pending_partial_exits <= 0:
                return
            slot.pending_partial_exits -= 1
            pos = slot.active_position
            direction = pos.direction
            entry_price = pos.entry_price
            sl_order_id = pos.sl_order_id
            new_trigger = pos.sl_price
            remaining_qty = pos.remaining_lots * slot.lot_size
            lot_size = slot.lot_size

        # Cancel old SL
        self._cancel_order(sl_order_id)

        # Place partial exit (1 lot)
        close_dir = "SELL" if direction == "BUY" else "BUY"
        ltp = slot.current_ltp
        if ltp <= 0:
            ltp = entry_price  # fallback
        buf = self._entry_buffer(ltp)
        price = self._round_tick(ltp - buf) if close_dir == "SELL" else self._round_tick(ltp + buf)

        try:
            order_id = self.client.kite.place_order(
                variety="regular", exchange="NFO",
                tradingsymbol=slot.trading_symbol,
                transaction_type=close_dir, quantity=lot_size,
                product=self.state.settings.product,
                order_type="LIMIT", price=price,
            )
            order_id = str(order_id)
        except Exception as e:
            logger.error(f"[{opt_type}] Partial exit order failed: {e}")
            new_sl_id = self._place_sl_order_for(
                slot.trading_symbol, direction, new_trigger,
                remaining_qty + lot_size,
            )
            with self._lock:
                if slot.active_position and new_sl_id:
                    slot.active_position.sl_order_id = new_sl_id
                    slot.active_position.remaining_lots += 1
                    self._save_state()
            return

        fill_price = self._get_fill_price(order_id, timeout=60, cancel_on_timeout=True)
        if fill_price is None:
            logger.error(f"[{opt_type}] Partial exit fill timeout — re-placing SL")
            new_sl_id = self._place_sl_order_for(
                slot.trading_symbol, direction, new_trigger, remaining_qty,
            )
            with self._lock:
                if slot.active_position and new_sl_id:
                    slot.active_position.sl_order_id = new_sl_id
                    self._save_state()
            return

        # Record partial trade
        self._record_partial_trade_slot(
            opt_type, direction, entry_price, fill_price, lot_size,
        )

        # Place new SL with reduced qty
        new_sl_id = self._place_sl_order_for(
            slot.trading_symbol, direction, new_trigger, remaining_qty,
        )

        with self._lock:
            if slot.active_position and new_sl_id:
                slot.active_position.sl_order_id = new_sl_id
                self._save_state()

        logger.info(
            f"[{opt_type}] Partial exit: {close_dir} 1 lot @ {fill_price}, "
            f"{slot.active_position.remaining_lots if slot.active_position else '?'} lots remain"
        )

    def _record_partial_trade_slot(self, opt_type, direction, entry_price,
                                   exit_price, actual_qty):
        """Record P&L for a partially exited lot on a slot."""
        slot = self.state.position_slots.get(opt_type)
        if not slot:
            return
        now = datetime.now(IST)
        pnl = (exit_price - entry_price) * actual_qty if direction == "BUY" else (entry_price - exit_price) * actual_qty
        trade = TradeRecord(
            direction=direction, entry_price=entry_price,
            exit_price=exit_price, entry_time="partial",
            exit_time=now.strftime("%H:%M:%S"),
            pnl=round(pnl, 2), quantity=1,
            date=now.strftime("%Y-%m-%d"),
            symbol=slot.trading_symbol,
        )
        with self._lock:
            self.state.trades_today.append(trade)
            self.state.total_pnl = round(self.state.total_pnl + pnl, 2)
            self._save_state()
        self._append_trade_history(trade)

    def _cancel_open_orders_for(self, symbol: str):
        """Cancel open orders for a specific trading symbol."""
        if not symbol:
            return
        try:
            orders = self.client.kite.orders()
            for o in orders:
                if (o["tradingsymbol"] == symbol
                        and o["status"] in ("OPEN", "TRIGGER PENDING")):
                    try:
                        self.client.kite.cancel_order(
                            variety=o.get("variety", "regular"),
                            order_id=o["order_id"],
                        )
                        logger.info(f"Cancelled stale order {o['order_id']} for {symbol}")
                    except Exception as e:
                        logger.warning(f"Failed to cancel order {o['order_id']}: {e}")
        except Exception as e:
            logger.error(f"Failed to fetch orders for {symbol} cleanup: {e}")

    def _place_sl_order_for(self, symbol: str, direction: str,
                            trigger_price: float, quantity: int) -> Optional[str]:
        """Place SL order for a specific symbol (used by signal-based slots)."""
        trigger_price = self._round_tick(trigger_price)
        sl_side = "SELL" if direction == "BUY" else "BUY"
        buf = self._sl_buffer(trigger_price)
        price = self._round_tick(trigger_price - buf) if sl_side == "SELL" else self._round_tick(trigger_price + buf)

        try:
            order_id = self.client.kite.place_order(
                variety="regular", exchange="NFO",
                tradingsymbol=symbol,
                transaction_type=sl_side, quantity=quantity,
                product=self.state.settings.product,
                order_type="SL",
                trigger_price=trigger_price, price=price,
            )
            logger.info(f"SL order for {symbol}: {sl_side} trigger={trigger_price} limit={price}, order_id={order_id}")
            return str(order_id)
        except Exception as e:
            logger.error(f"SL order for {symbol} failed: {e}")
            return None

    def _modify_sl_order_slot(self, opt_type: str, order_id: str,
                              new_trigger: float) -> bool:
        """Trail SL order for a specific slot."""
        new_trigger = self._round_tick(new_trigger)
        slot = self.state.position_slots.get(opt_type)
        if not slot or not slot.active_position:
            return False

        direction = slot.active_position.direction
        sl_side = "SELL" if direction == "BUY" else "BUY"
        buf = self._sl_buffer(new_trigger)
        new_price = self._round_tick(new_trigger - buf) if sl_side == "SELL" else self._round_tick(new_trigger + buf)

        try:
            self.client.kite.modify_order(
                variety="regular", order_id=order_id,
                trigger_price=new_trigger, price=new_price,
            )
            logger.info(f"[{opt_type}] SL modified: trigger={new_trigger}, limit={new_price}")
            return True
        except Exception as e:
            logger.error(f"[{opt_type}] SL modify failed: {e}")
            return False

    def _square_off_slot(self, opt_type: str):
        """Square off a single position slot at EOD."""
        slot = self.state.position_slots.get(opt_type)
        if not slot or not slot.active_position:
            return

        pos = slot.active_position
        if pos.sl_order_id:
            self._cancel_order(pos.sl_order_id)

        remaining_lots = pos.remaining_lots if pos.remaining_lots > 0 else self.state.settings.quantity
        remaining_qty = remaining_lots * slot.lot_size
        close_dir = "SELL" if pos.direction == "BUY" else "BUY"

        ltp = slot.current_ltp
        if ltp <= 0:
            try:
                sym = f"NFO:{slot.trading_symbol}"
                data = self.client.kite.ltp(sym)
                ltp = data[sym]["last_price"]
            except Exception:
                ltp = pos.entry_price

        buf = self._entry_buffer(ltp)
        price = self._round_tick(ltp - buf) if close_dir == "SELL" else self._round_tick(ltp + buf)

        try:
            order_id = self.client.kite.place_order(
                variety="regular", exchange="NFO",
                tradingsymbol=slot.trading_symbol,
                transaction_type=close_dir, quantity=remaining_qty,
                product=self.state.settings.product,
                order_type="LIMIT", price=price,
            )
            order_id = str(order_id)
        except Exception as e:
            logger.error(f"[{opt_type}] Square-off order failed: {e}")
            return

        fill_price = self._get_fill_price(order_id, timeout=30, cancel_on_timeout=True)
        if fill_price is not None:
            self._close_position_slot(opt_type, exit_price=fill_price)
        else:
            # Retry with aggressive buffer
            logger.warning(f"[{opt_type}] Square-off attempt 1 failed — retrying with wider buffer")
            price2 = self._round_tick(ltp - self.SQUARE_OFF_BUFFER) if close_dir == "SELL" else self._round_tick(ltp + self.SQUARE_OFF_BUFFER)
            try:
                order_id2 = self.client.kite.place_order(
                    variety="regular", exchange="NFO",
                    tradingsymbol=slot.trading_symbol,
                    transaction_type=close_dir, quantity=remaining_qty,
                    product=self.state.settings.product,
                    order_type="LIMIT", price=price2,
                )
                order_id2 = str(order_id2)
                fill_price2 = self._get_fill_price(order_id2, timeout=60, cancel_on_timeout=True)
                if fill_price2 is not None:
                    self._close_position_slot(opt_type, exit_price=fill_price2)
                else:
                    logger.error(f"[{opt_type}] SQUARE-OFF FAILED: manual intervention required")
            except Exception as e:
                logger.error(f"[{opt_type}] Aggressive square-off failed: {e}")

    def _square_off_all_slots(self):
        """Square off all active position slots."""
        for opt in ("CE", "PE"):
            slot = self.state.position_slots.get(opt)
            if slot and slot.active_position:
                logger.info(f"[{opt}] Squaring off at EOD")
                self._square_off_slot(opt)

    # ── Public API ──────────────────────────────────────────────────────

    def start(self):
        """Start the strategy engine in a background thread.

        Always starts with a clean slate — no old positions, SL values,
        or trades carried over.  Fresh instrument selection + fresh orders
        based on current settings.
        """
        if self._running:
            logger.warning("Engine already running")
            return

        self._running = True
        self._pending_direction = None
        self._recovery_mode = False
        self._pending_partial_exits = 0

        with self._lock:
            self.state.settings.enabled = True
            # Clean slate: fresh positions, keep cumulative P&L from history
            self.state.active_position = None
            self.state.trading_symbol = ""
            self.state.instrument_token = 0
            self.state.strategy_data = {}
            self.state.current_ltp = 0.0
            self.state.position_slots = {}
            # Rebuild today's cumulative P&L from history
            self.state.trades_today, self.state.total_pnl = self._load_todays_trades()
            self.state.status_message = "Starting fresh..."
            self.state.last_date = date.today().isoformat()
            self._save_state()

        self._thread = threading.Thread(
            target=self._engine_loop, daemon=True, name="strategy-engine"
        )
        self._thread.start()
        logger.info("Strategy engine started")

    def stop(self):
        """Stop the engine, square off, and disable."""
        self._running = False

        if self._thread:
            self._thread.join(timeout=15)
            self._thread = None

        # Square off signal-based slots
        if self._strategy and self._strategy.signal_based:
            self._square_off_all_slots()
        elif self.state.active_position:
            self._square_off()

        self._stop_ticker()

        with self._lock:
            self.state.engine_status = "STOPPED"
            self.state.status_message = "Stopped by user"
            self.state.settings.enabled = False
            # Clear instrument so next start re-selects fresh
            self.state.trading_symbol = ""
            self.state.instrument_token = 0
            self.state.position_slots = {}
            self._save_state()

        logger.info("Strategy engine stopped")

    def update_settings(self, **kwargs):
        """Update strategy settings from the dashboard."""
        with self._lock:
            for key, value in kwargs.items():
                if hasattr(self.state.settings, key):
                    setattr(self.state.settings, key, value)
            # Enforce quantity (lots) is a multiple of lot_multiplier
            min_lots = self._strategy.lot_multiplier
            qty = self.state.settings.quantity
            if qty < min_lots or qty % min_lots != 0:
                self.state.settings.quantity = min_lots
                logger.info(f"Lots auto-corrected to {min_lots} "
                            f"(multiplier={self._strategy.lot_multiplier})")
            self._save_state()
        logger.info(f"Settings updated: {kwargs}")

    @property
    def is_running(self) -> bool:
        return self._running
