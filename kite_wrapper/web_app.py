"""Flask web app for hosted Kite authentication."""

import logging

from flask import Flask, redirect, request, render_template_string
from kiteconnect import KiteConnect

from .client import KiteClient
from .config import get_settings
from .strategy import StrategyEngine
from .token_manager import TokenManager
from .dashboard import dashboard_bp, init_dashboard

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)

app = Flask(__name__)

# Shared instances used by the strategy engine and dashboard
_client = KiteClient()
_engine = StrategyEngine(_client)
init_dashboard(_engine)
app.register_blueprint(dashboard_bp)

HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Kite Trading - Auth</title>
    <style>
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            max-width: 600px;
            margin: 100px auto;
            padding: 20px;
            text-align: center;
        }
        .status {
            padding: 20px;
            border-radius: 8px;
            margin: 20px 0;
        }
        .success { background: #d4edda; color: #155724; }
        .error { background: #f8d7da; color: #721c24; }
        .info { background: #e7f1ff; color: #004085; }
        .btn {
            display: inline-block;
            padding: 12px 24px;
            background: #387ed1;
            color: white;
            text-decoration: none;
            border-radius: 6px;
            font-size: 16px;
        }
        .btn:hover { background: #2c6ab8; }
    </style>
</head>
<body>
    <h1>Kite Trading</h1>
    {% if status == 'authenticated' %}
        <div class="status success">
            <h2>Authenticated</h2>
            <p>Logged in as: <strong>{{ user_id }}</strong></p>
            <p>Token valid until ~6 AM IST tomorrow</p>
        </div>
        <a href="/login" class="btn">Re-authenticate</a>
    {% elif status == 'success' %}
        <div class="status success">
            <h2>Login Successful</h2>
            <p>Welcome, <strong>{{ user_id }}</strong>!</p>
            <p>Token saved. You can close this window.</p>
        </div>
    {% elif status == 'error' %}
        <div class="status error">
            <h2>Error</h2>
            <p>{{ message }}</p>
        </div>
        <a href="/login" class="btn">Try Again</a>
    {% else %}
        <div class="status info">
            <p>Not authenticated or token expired</p>
        </div>
        <a href="/login" class="btn">Login with Kite</a>
    {% endif %}
    <div style="margin-top: 20px;">
        <a href="/dashboard" class="btn" style="background: #28a745;">Trading Dashboard &rarr;</a>
    </div>
</body>
</html>
"""


def get_kite() -> KiteConnect:
    """Get KiteConnect instance."""
    settings = get_settings()
    return KiteConnect(api_key=settings.kite_api_key)


@app.route("/")
def index():
    """Show current auth status."""
    settings = get_settings()
    token_manager = TokenManager(settings.kite_token_file)
    token = token_manager.load_token()

    if token:
        kite = get_kite()
        kite.set_access_token(token)
        try:
            profile = kite.profile()
            return render_template_string(
                HTML_TEMPLATE,
                status="authenticated",
                user_id=profile.get("user_id", "Unknown"),
            )
        except Exception:
            pass  # Token invalid, show login

    return render_template_string(HTML_TEMPLATE, status="none")


@app.route("/login")
def login():
    """Redirect to Kite login page."""
    kite = get_kite()
    return redirect(kite.login_url())


@app.route("/callback")
def callback():
    """Handle OAuth callback from Kite."""
    settings = get_settings()
    token_manager = TokenManager(settings.kite_token_file)

    error = request.args.get("error")
    if error:
        return render_template_string(HTML_TEMPLATE, status="error", message=error)

    request_token = request.args.get("request_token")
    if not request_token:
        return render_template_string(
            HTML_TEMPLATE, status="error", message="No request token received"
        )

    try:
        kite = get_kite()
        data = kite.generate_session(request_token, api_secret=settings.kite_api_secret)

        access_token = data["access_token"]
        user_id = data.get("user_id", "")

        token_manager.save_token(access_token, user_id)

        # Sync token with the shared client so the strategy engine can use it
        _client.kite.set_access_token(access_token)

        return render_template_string(HTML_TEMPLATE, status="success", user_id=user_id)

    except Exception as e:
        return render_template_string(HTML_TEMPLATE, status="error", message=str(e))


@app.route("/status")
def status():
    """API endpoint to check auth status (for scripts)."""
    settings = get_settings()
    token_manager = TokenManager(settings.kite_token_file)
    token = token_manager.load_token()

    if token:
        kite = get_kite()
        kite.set_access_token(token)
        try:
            profile = kite.profile()
            return {"authenticated": True, "user_id": profile.get("user_id")}
        except Exception:
            pass

    return {"authenticated": False}


# Auto-start the strategy engine if it was previously enabled and the client
# has a valid token.  This covers server-restart scenarios.
if _engine.state.settings.enabled and _client.is_authenticated:
    _engine.start()


def run_server(host: str = "0.0.0.0", port: int = 5000, debug: bool = False):
    """Run the web server."""
    app.run(host=host, port=port, debug=debug)


if __name__ == "__main__":
    run_server()
