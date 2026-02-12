"""Token persistence and expiry management."""

import json
import os
from datetime import datetime
from pathlib import Path

import pytz

IST = pytz.timezone("Asia/Kolkata")


class TokenManager:
    """Manages access token persistence and expiry checks."""

    def __init__(self, token_file: Path):
        self.token_file = Path(token_file).expanduser()

    def save_token(self, access_token: str, user_id: str = "") -> None:
        """Save access token with timestamp. Sets file permissions to 0600."""
        data = {
            "access_token": access_token,
            "user_id": user_id,
            "saved_at": datetime.now(IST).isoformat(),
        }
        self.token_file.write_text(json.dumps(data, indent=2))
        os.chmod(self.token_file, 0o600)

    def load_token(self) -> str | None:
        """Load access token if valid (not expired). Returns None if expired or missing."""
        if not self.token_file.exists():
            return None

        try:
            data = json.loads(self.token_file.read_text())
            saved_at = datetime.fromisoformat(data["saved_at"])

            if self._is_expired(saved_at):
                return None

            return data.get("access_token")
        except (json.JSONDecodeError, KeyError):
            return None

    def _is_expired(self, saved_at: datetime) -> bool:
        """Check if token is expired. Tokens expire daily at ~6 AM IST (use 7:30 AM as margin)."""
        now = datetime.now(IST)
        today_expiry = now.replace(hour=7, minute=30, second=0, microsecond=0)

        # If saved before today's expiry time and we're past it, token is expired
        if saved_at.tzinfo is None:
            saved_at = IST.localize(saved_at)

        if now >= today_expiry and saved_at < today_expiry:
            return True

        # If saved yesterday or earlier, expired
        if saved_at.date() < now.date():
            return True

        return False

    def clear_token(self) -> None:
        """Delete stored token."""
        if self.token_file.exists():
            self.token_file.unlink()
