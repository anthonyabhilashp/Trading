"""CSV cache for historical candle data — read/write helpers for backtest engine."""

import csv
from datetime import datetime
from pathlib import Path

import pytz

IST = pytz.timezone("Asia/Kolkata")

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
NIFTY_DIR = DATA_DIR / "nifty_index"
OPTIONS_DIR = DATA_DIR / "options"

_CSV_FIELDS = ["date", "open", "high", "low", "close", "volume"]


# ── Path helpers ──────────────────────────────────────────────────────────────

def nifty_day_path(date_str: str) -> Path:
    return NIFTY_DIR / f"{date_str}_day.csv"


def nifty_minute_path(date_str: str) -> Path:
    return NIFTY_DIR / f"{date_str}_minute.csv"


def option_minute_path(date_str: str, tradingsymbol: str) -> Path:
    return OPTIONS_DIR / date_str / f"{tradingsymbol}_minute.csv"


# ── Existence checks ─────────────────────────────────────────────────────────

def has_nifty_day(date_str: str) -> bool:
    return nifty_day_path(date_str).is_file()


def has_nifty_minute(date_str: str, min_candles: int = 300) -> bool:
    """Check if NIFTY minute CSV exists and has enough candles (not truncated)."""
    p = nifty_minute_path(date_str)
    if not p.is_file():
        return False
    # Quick line count to detect incomplete downloads
    with open(p) as f:
        lines = sum(1 for _ in f)
    return lines > min_candles  # header + candles (full day ~376)


def has_option_minute(date_str: str, tradingsymbol: str) -> bool:
    return option_minute_path(date_str, tradingsymbol).is_file()


# ── Write ─────────────────────────────────────────────────────────────────────

def _write_candles(path: Path, candles: list[dict]):
    """Write candle list to CSV. Creates parent dirs as needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=_CSV_FIELDS)
        writer.writeheader()
        for c in candles:
            row = {
                "date": _format_date(c["date"]),
                "open": c["open"],
                "high": c["high"],
                "low": c["low"],
                "close": c["close"],
                "volume": c.get("volume", 0),
            }
            writer.writerow(row)


def _format_date(dt) -> str:
    """Format datetime to string with timezone info."""
    if hasattr(dt, "strftime"):
        if dt.tzinfo is None:
            dt = IST.localize(dt)
        return dt.strftime("%Y-%m-%d %H:%M:%S%z")
    return str(dt)


def save_nifty_day(date_str: str, candles: list[dict]):
    _write_candles(nifty_day_path(date_str), candles)


def save_nifty_minute(date_str: str, candles: list[dict]):
    _write_candles(nifty_minute_path(date_str), candles)


def save_option_minute(date_str: str, tradingsymbol: str, candles: list[dict]):
    _write_candles(option_minute_path(date_str, tradingsymbol), candles)


# ── Read ──────────────────────────────────────────────────────────────────────

def _parse_row(row: dict) -> dict:
    """Parse a CSV row into a candle dict with proper types."""
    dt_str = row["date"]
    # Parse datetime — handles formats like "2025-02-20 09:15:00+0530"
    try:
        dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S%z")
    except ValueError:
        # Fallback for date-only rows (day candles)
        try:
            dt = datetime.strptime(dt_str[:10], "%Y-%m-%d")
            dt = IST.localize(dt)
        except ValueError:
            dt = dt_str

    return {
        "date": dt,
        "open": float(row["open"]),
        "high": float(row["high"]),
        "low": float(row["low"]),
        "close": float(row["close"]),
        "volume": int(float(row["volume"])),
    }


def _read_candles(path: Path) -> list[dict] | None:
    """Read CSV back to candle dicts. Returns None if file doesn't exist."""
    if not path.is_file():
        return None
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        return [_parse_row(row) for row in reader]


def load_nifty_day(date_str: str) -> list[dict] | None:
    return _read_candles(nifty_day_path(date_str))


def load_nifty_minute(date_str: str) -> list[dict] | None:
    return _read_candles(nifty_minute_path(date_str))


def load_option_minute(date_str: str, tradingsymbol: str) -> list[dict] | None:
    return _read_candles(option_minute_path(date_str, tradingsymbol))


# ── List cached dates ─────────────────────────────────────────────────────────

def list_cached_dates() -> dict:
    """Scan data/ and return summary of what's downloaded.

    Returns dict like:
        {
            "dates": ["2025-02-20", "2025-02-21", ...],
            "detail": {
                "2025-02-20": {"nifty_day": True, "nifty_minute": True, "options": 62},
                ...
            }
        }
    """
    dates = set()
    detail = {}

    # Scan nifty_index dir for dates
    if NIFTY_DIR.is_dir():
        for f in NIFTY_DIR.iterdir():
            if f.name.endswith("_day.csv"):
                d = f.name.replace("_day.csv", "")
                dates.add(d)
            elif f.name.endswith("_minute.csv"):
                d = f.name.replace("_minute.csv", "")
                dates.add(d)

    # Scan options dir for dates
    if OPTIONS_DIR.is_dir():
        for d_dir in OPTIONS_DIR.iterdir():
            if d_dir.is_dir():
                dates.add(d_dir.name)

    sorted_dates = sorted(dates)
    for d in sorted_dates:
        opt_count = 0
        opt_dir = OPTIONS_DIR / d
        if opt_dir.is_dir():
            opt_count = sum(1 for f in opt_dir.iterdir() if f.suffix == ".csv")
        detail[d] = {
            "nifty_day": has_nifty_day(d),
            "nifty_minute": has_nifty_minute(d),
            "options": opt_count,
        }

    return {"dates": sorted_dates, "detail": detail}
