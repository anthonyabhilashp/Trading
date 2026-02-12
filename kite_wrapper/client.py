"""Main client wrapper around KiteConnect with auto-auth."""

from kiteconnect import KiteConnect, KiteTicker

from .config import Settings, get_settings
from .token_manager import TokenManager
from .auth_server import run_auth_server


class KiteClient:
    """
    Wrapper around KiteConnect with automatic token management.

    Usage:
        client = KiteClient()

        # If token exists and valid, ready to use
        if client.is_authenticated:
            print(client.kite.profile())

        # Otherwise, trigger login flow
        else:
            client.login()  # Opens browser
            print(client.kite.profile())

        # Access underlying KiteConnect instance for all API calls
        client.kite.orders()
        client.kite.positions()
        client.kite.holdings()
        # etc.
    """

    def __init__(self, settings: Settings | None = None):
        self.settings = settings or get_settings()
        self.token_manager = TokenManager(self.settings.kite_token_file)

        self.kite = KiteConnect(api_key=self.settings.kite_api_key)
        self._ticker: KiteTicker | None = None

        # Try to load existing token
        token = self.token_manager.load_token()
        if token:
            self.kite.set_access_token(token)

    @property
    def is_authenticated(self) -> bool:
        """Check if we have a valid access token."""
        return self.kite.access_token is not None

    def login(self) -> None:
        """
        Perform OAuth login flow.
        Opens browser, captures callback, exchanges for access token.
        """
        login_url = self.kite.login_url()
        result = run_auth_server(login_url)

        if result.error:
            raise RuntimeError(f"Authentication failed: {result.error}")

        if not result.request_token:
            raise RuntimeError("No request token received")

        # Exchange request_token for access_token
        data = self.kite.generate_session(
            result.request_token,
            api_secret=self.settings.kite_api_secret,
        )

        access_token = data["access_token"]
        user_id = data.get("user_id", "")

        self.kite.set_access_token(access_token)
        self.token_manager.save_token(access_token, user_id)

        print(f"Logged in as: {user_id}")

    def logout(self) -> None:
        """Invalidate session and clear stored token."""
        try:
            self.kite.invalidate_access_token()
        except Exception:
            pass  # Token might already be invalid
        self.token_manager.clear_token()
        self.kite.access_token = None

    def get_ticker(self) -> KiteTicker:
        """Get KiteTicker instance for WebSocket streaming."""
        if not self.is_authenticated:
            raise RuntimeError("Not authenticated. Call login() first.")

        if self._ticker is None:
            self._ticker = KiteTicker(
                api_key=self.settings.kite_api_key,
                access_token=self.kite.access_token,
            )
        return self._ticker
