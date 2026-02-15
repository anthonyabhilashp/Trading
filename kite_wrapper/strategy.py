"""NIFTY Options SAR (Stop-and-Reverse) Trading Strategy Engine."""

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
    quantity: int = 0  # auto-set from lot_size on instrument selection
    product: str = "NRML"
    target_premium: float = 1000.0  # pick strike with premium closest to this


@dataclass
class ActivePosition:
    direction: str = ""  # "SELL" or "BUY"
    entry_price: float = 0.0
    sl_price: float = 0.0
    target_price: float = 0.0
    sl_order_id: str = ""
    order_id: str = ""
    entry_time: str = ""


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
class StrategyState:
    settings: StrategySettings = field(default_factory=StrategySettings)
    engine_status: str = "STOPPED"
    trading_symbol: str = ""
    instrument_token: int = 0
    lot_size: int = 0
    active_position: Optional[ActivePosition] = None
    trades_today: list = field(default_factory=list)
    total_pnl: float = 0.0
    current_ltp: float = 0.0
    last_date: str = ""


# ─── Serialization Helpers ──────────────────────────────────────────────────


def _state_to_dict(state: StrategyState) -> dict:
    return {
        "settings": asdict(state.settings),
        "engine_status": state.engine_status,
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
    }


def _state_from_dict(d: dict) -> StrategyState:
    state = StrategyState()
    if "settings" in d:
        state.settings = StrategySettings(**d["settings"])
    state.engine_status = d.get("engine_status", "STOPPED")
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
    return state


# ─── Strategy Engine ────────────────────────────────────────────────────────


class StrategyEngine:
    """SAR trading engine — sells ATM NIFTY CE, trails SL, reverses on SL hit."""

    def __init__(self, client: KiteClient):
        self.client = client
        self._lock = threading.Lock()
        self._thread: Optional[threading.Thread] = None
        self._running = False
        self._ticker = None
        self._reversal_in_progress = False
        self._pending_direction: Optional[str] = None

        self.state = self._load_state()

    # ── State Persistence ───────────────────────────────────────────────

    def _load_state(self) -> StrategyState:
        if STATE_FILE.exists():
            try:
                data = json.loads(STATE_FILE.read_text())
                state = _state_from_dict(data)
                today = date.today().isoformat()
                if state.last_date != today:
                    state.trades_today = []
                    state.total_pnl = 0.0
                    state.active_position = None
                    state.trading_symbol = ""
                    state.instrument_token = 0
                    # Keep lot_size and settings across days — lot_size
                    # rarely changes and gets refreshed on instrument selection
                    state.last_date = today
                    state.engine_status = "STOPPED"
                return state
            except Exception as e:
                logger.error(f"Failed to load state: {e}")
        state = StrategyState()
        state.last_date = date.today().isoformat()
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

    # ── Instrument Selection ────────────────────────────────────────────

    def _select_instrument(self) -> bool:
        """Pick NIFTY CE monthly contract with premium closest to target."""
        try:
            instruments = self.client.kite.instruments("NFO")
            today = date.today()
            min_expiry = today + timedelta(days=30)

            # All NIFTY CE with expiry >= 30 days
            all_ce = [
                i for i in instruments
                if i["name"] == "NIFTY"
                and i["instrument_type"] == "CE"
                and i["expiry"] >= min_expiry
            ]
            if not all_ce:
                logger.error(f"No NIFTY CE with expiry >= {min_expiry}")
                return False

            # Identify monthly expiries (latest expiry per calendar month)
            all_expiries = {i["expiry"] for i in instruments
                           if i["name"] == "NIFTY"
                           and i["instrument_type"] == "CE"}
            month_max = {}
            for exp in all_expiries:
                key = (exp.year, exp.month)
                if key not in month_max or exp > month_max[key]:
                    month_max[key] = exp
            monthly_expiries = set(month_max.values())

            # Filter to monthly, pick the nearest monthly expiry
            monthly_ce = [i for i in all_ce if i["expiry"] in monthly_expiries]
            if not monthly_ce:
                logger.warning("No monthly expiry found, falling back to all")
                monthly_ce = all_ce

            target_expiry = min(i["expiry"] for i in monthly_ce)
            candidates = [i for i in monthly_ce if i["expiry"] == target_expiry]
            logger.info(f"Monthly expiry: {target_expiry}, "
                        f"{len(candidates)} strikes available")

            # Fetch LTP for all strikes of this expiry to find target premium
            sym_map = {}  # "NFO:SYMBOL" → instrument dict
            for c in candidates:
                sym_map[f"NFO:{c['tradingsymbol']}"] = c

            # kite.ltp() supports batches; query all at once
            ltp_data = self.client.kite.ltp(list(sym_map.keys()))

            target = self.state.settings.target_premium
            best = None
            best_diff = float("inf")
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
                return False

            # Accept if within ±20% of target, otherwise warn
            if best_diff > target * 0.2:
                logger.warning(
                    f"Closest premium {best_premium:.1f} is far from "
                    f"target {target:.0f}, proceeding anyway"
                )

            chosen = best
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
                if self.state.settings.quantity < lot or self.state.settings.quantity % lot != 0:
                    self.state.settings.quantity = lot
                self._save_state()

            logger.info(
                f"Selected: {chosen['tradingsymbol']} "
                f"(strike: {chosen['strike']}, premium: {best_premium:.1f}, "
                f"expiry: {expiry}, lot: {lot})"
            )
            return True

        except Exception as e:
            logger.error(f"Instrument selection failed: {e}")
            return False

    # ── Order Management ────────────────────────────────────────────────

    # NIFTY option tick size
    TICK = 0.05
    # Limit‑price buffer for entry orders (points beyond LTP)
    ENTRY_BUFFER = 2.0
    # Limit‑price buffer for SL orders (points beyond trigger)
    SL_BUFFER = 10.0

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

    def _place_entry_order(self, direction: str) -> Optional[str]:
        """Place a LIMIT order near LTP. Returns order_id or None."""
        ltp = self._get_ltp()
        if ltp <= 0:
            logger.error("Cannot place entry: no LTP available")
            return None

        # Aggressive limit to ensure quick fill
        if direction == "SELL":
            price = self._round_tick(ltp - self.ENTRY_BUFFER)
        else:
            price = self._round_tick(ltp + self.ENTRY_BUFFER)

        try:
            order_id = self.client.kite.place_order(
                variety="regular",
                exchange="NFO",
                tradingsymbol=self.state.trading_symbol,
                transaction_type=direction,
                quantity=self.state.settings.quantity,
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

    def _get_fill_price(self, order_id: str, timeout: int = 30) -> Optional[float]:
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
        logger.error(f"Timeout waiting for fill on order {order_id}")
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

    def _place_sl_order(self, direction: str, trigger_price: float) -> Optional[str]:
        """Place SL (stop-loss limit) protection order on exchange."""
        # SELL position → BUY SL, BUY position → SELL SL
        sl_side = "BUY" if direction == "SELL" else "SELL"

        # Wide limit buffer so the order fills even on a gap
        if sl_side == "BUY":
            price = self._round_tick(trigger_price + self.SL_BUFFER)
        else:
            price = self._round_tick(trigger_price - self.SL_BUFFER)

        try:
            order_id = self.client.kite.place_order(
                variety="regular",
                exchange="NFO",
                tradingsymbol=self.state.trading_symbol,
                transaction_type=sl_side,
                quantity=self.state.settings.quantity,
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
        with self._lock:
            pos = self.state.active_position
            if not pos:
                return False
            direction = pos.direction

        sl_side = "BUY" if direction == "SELL" else "SELL"
        if sl_side == "BUY":
            new_price = self._round_tick(new_trigger + self.SL_BUFFER)
        else:
            new_price = self._round_tick(new_trigger - self.SL_BUFFER)

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

    # ── Position Entry / Exit / Reversal ────────────────────────────────

    def _enter_position(self, direction: str):
        """Enter a new position with SL protection."""
        # Clean up stale orders that may be blocking margin
        self._cancel_open_orders()

        order_id = self._place_entry_order(direction)
        if not order_id:
            return

        fill_price = self._get_fill_price(order_id)
        if fill_price is None:
            return

        sl_pts = self.state.settings.sl_points
        tgt_pts = self.state.settings.target_points

        if direction == "SELL":
            sl_price = fill_price + sl_pts
            target_price = fill_price - tgt_pts
        else:
            sl_price = fill_price - sl_pts
            target_price = fill_price + tgt_pts

        sl_order_id = self._place_sl_order(direction, sl_price)
        if not sl_order_id:
            logger.error("SL-M placement failed — closing entry immediately")
            close_dir = "BUY" if direction == "SELL" else "SELL"
            self._place_entry_order(close_dir)
            return

        now_str = datetime.now(IST).strftime("%H:%M:%S")

        with self._lock:
            self.state.active_position = ActivePosition(
                direction=direction,
                entry_price=fill_price,
                sl_price=sl_price,
                target_price=target_price,
                sl_order_id=sl_order_id,
                order_id=order_id,
                entry_time=now_str,
            )
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

            if pos.direction == "SELL":
                pnl = (pos.entry_price - exit_price) * self.state.settings.quantity
            else:
                pnl = (exit_price - pos.entry_price) * self.state.settings.quantity

            trade = TradeRecord(
                direction=pos.direction,
                entry_price=pos.entry_price,
                exit_price=exit_price,
                entry_time=pos.entry_time,
                exit_time=now_str,
                pnl=round(pnl, 2),
                quantity=self.state.settings.quantity,
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

    def _do_reversal(self, sl_fill_price: float):
        """SL hit → close current trade, enter reversed direction."""
        with self._lock:
            pos = self.state.active_position
            if not pos:
                return
            old_direction = pos.direction

        self._close_position(exit_price=sl_fill_price)

        new_direction = "BUY" if old_direction == "SELL" else "SELL"
        logger.info(f"Reversing: {old_direction} → {new_direction}")
        self._pending_direction = new_direction
        self._enter_position(new_direction)

    def _square_off(self):
        """Cancel SL-M and close position at market (EOD)."""
        with self._lock:
            pos = self.state.active_position
            if not pos:
                return
            sl_order_id = pos.sl_order_id
            direction = pos.direction

        if sl_order_id:
            self._cancel_order(sl_order_id)

        close_dir = "BUY" if direction == "SELL" else "SELL"
        order_id = self._place_entry_order(close_dir)
        if order_id:
            fill_price = self._get_fill_price(order_id)
            if fill_price is not None:
                self._close_position(exit_price=fill_price)
                return

        self._close_position(exit_price=0.0)

    # ── Tick Handling (Trailing Logic) ──────────────────────────────────

    def _handle_tick(self, ltp: float):
        """Check if target hit → trail SL. Called from ticker thread."""
        sl_order_id = None
        new_trigger = None

        with self._lock:
            self.state.current_ltp = ltp
            pos = self.state.active_position
            if not pos:
                return

            sl_pts = self.state.settings.sl_points
            tgt_pts = self.state.settings.target_points
            trailed = False

            if pos.direction == "SELL":
                while ltp <= pos.target_price:
                    pos.target_price -= tgt_pts
                    pos.sl_price -= sl_pts
                    trailed = True
            elif pos.direction == "BUY":
                while ltp >= pos.target_price:
                    pos.target_price += tgt_pts
                    pos.sl_price += sl_pts
                    trailed = True

            if trailed:
                sl_order_id = pos.sl_order_id
                new_trigger = pos.sl_price
                self._save_state()

        if trailed and sl_order_id:
            self._modify_sl_order(sl_order_id, new_trigger)
            logger.info(f"Trailed SL → {new_trigger}, target → {pos.target_price}")

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
                self._do_reversal(fill_price)
            finally:
                self._reversal_in_progress = False

    # ── WebSocket Ticker ────────────────────────────────────────────────

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
        self._ticker = self.client.get_ticker()

        def on_ticks(ws, ticks):
            for tick in ticks:
                if tick["instrument_token"] == token:
                    self._handle_tick(tick["last_price"])

        def on_connect(ws, response):
            ws.subscribe([token])
            ws.set_mode(ws.MODE_LTP, [token])
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

        self._ticker.connect(threaded=True)

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
            self._save_state()

        # Crash recovery
        recovered = False
        if self.state.active_position and self.client.is_authenticated:
            recovered = self._attempt_recovery()
            if recovered:
                with self._lock:
                    self.state.engine_status = "ACTIVE"
                    self._save_state()
                self._start_ticker()

        entry_failures = 0

        while self._running:
            now = datetime.now(IST)
            current_time = now.strftime("%H:%M")

            # ── Past stop time → square off and exit ──
            if current_time >= self.state.settings.stop_time:
                if self.state.active_position:
                    logger.info("Stop time reached — squaring off")
                    self._square_off()
                self._stop_ticker()
                with self._lock:
                    self.state.engine_status = "MARKET_CLOSED"
                    self._save_state()
                break

            # ── Before start time → wait ──
            if current_time < self.state.settings.start_time:
                with self._lock:
                    if self.state.engine_status != "WAITING":
                        self.state.engine_status = "WAITING"
                        self._save_state()
                time.sleep(5)
                continue

            # ── Trading hours ──
            if not self.client.is_authenticated:
                logger.error("Not authenticated — stopping engine")
                with self._lock:
                    self.state.engine_status = "STOPPED"
                    self._save_state()
                break

            if self.state.engine_status != "ACTIVE":
                with self._lock:
                    self.state.engine_status = "ACTIVE"
                    self._save_state()

            # Select instrument once per day
            if not self.state.trading_symbol:
                if not self._select_instrument():
                    logger.error("Instrument selection failed — stopping")
                    with self._lock:
                        self.state.engine_status = "STOPPED"
                        self._save_state()
                    break

            # Start ticker once
            if not self._ticker:
                self._start_ticker()
                time.sleep(2)

            # Enter position if none (initial SELL or retry after failed reversal)
            if not self.state.active_position and not self._reversal_in_progress:
                if entry_failures >= 5:
                    logger.error("5 consecutive entry failures — stopping engine")
                    with self._lock:
                        self.state.engine_status = "STOPPED"
                        self._save_state()
                    break
                direction = self._pending_direction or "SELL"
                self._pending_direction = None
                self._enter_position(direction)
                if not self.state.active_position:
                    entry_failures += 1
                    logger.warning(f"Entry attempt {entry_failures}/5 failed, "
                                   f"retrying in 10s")
                    time.sleep(10)
                    continue
                entry_failures = 0

            time.sleep(2)

        self._running = False
        logger.info("Engine loop exited")

    # ── Public API ──────────────────────────────────────────────────────

    def start(self):
        """Start the strategy engine in a background thread."""
        if self._running:
            logger.warning("Engine already running")
            return

        self._running = True
        self._pending_direction = None

        with self._lock:
            self.state.settings.enabled = True
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

        if self.state.active_position:
            self._square_off()

        self._stop_ticker()

        with self._lock:
            self.state.engine_status = "STOPPED"
            self.state.settings.enabled = False
            self._save_state()

        logger.info("Strategy engine stopped")

    def update_settings(self, **kwargs):
        """Update strategy settings from the dashboard."""
        with self._lock:
            for key, value in kwargs.items():
                if hasattr(self.state.settings, key):
                    setattr(self.state.settings, key, value)
            self._save_state()
        logger.info(f"Settings updated: {kwargs}")

    @property
    def is_running(self) -> bool:
        return self._running
