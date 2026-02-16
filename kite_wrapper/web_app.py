"""Flask web app for hosted Kite authentication."""

import logging
from collections import deque
from pathlib import Path

from flask import Flask, jsonify, redirect, request, render_template_string
from kiteconnect import KiteConnect

from .client import KiteClient
from .config import get_settings
from .strategy import StrategyEngine
from .token_manager import TokenManager
from .dashboard import dashboard_bp, init_dashboard
import kite_wrapper.strategies  # noqa: F401 — triggers @register_strategy decorators

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
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Kite Trading — Login</title>
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    background: #f0f2f5; color: #1a1a2e; line-height: 1.5;
    min-height: 100vh; display: flex; align-items: center; justify-content: center;
  }
  .container { max-width: 420px; width: 100%; padding: 20px; }
  .card {
    background: #fff; border-radius: 10px; padding: 32px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.08); text-align: center;
  }
  h1 { font-size: 22px; font-weight: 700; margin-bottom: 24px; }
  .status-indicator {
    display: inline-flex; align-items: center; gap: 8px;
    font-size: 15px; font-weight: 600; margin-bottom: 16px;
  }
  .dot { width: 10px; height: 10px; border-radius: 50%; flex-shrink: 0; }
  .dot.red  { background: #dc3545; }
  .dot.blue { background: #387ed1; }
  .info { font-size: 14px; color: #999; margin-bottom: 8px; }
  .alert {
    padding: 14px 16px; border-radius: 8px; margin: 16px 0;
    font-size: 14px; text-align: left;
    background: #f8d7da; color: #721c24;
  }
  .btn-group { display: flex; flex-direction: column; gap: 10px; margin-top: 24px; }
  .btn {
    display: block; width: 100%; padding: 12px 24px; border: none; border-radius: 6px;
    font-size: 15px; font-weight: 600; cursor: pointer; text-decoration: none;
    text-align: center; min-height: 44px; transition: opacity 0.15s;
  }
  .btn:hover { opacity: 0.85; }
  .btn-primary   { background: #387ed1; color: #fff; }
  .btn-secondary { background: #e9ecef; color: #1a1a2e; }
  @media (max-width: 640px) {
    .container { padding: 12px; }
    .card { padding: 24px 20px; }
  }
</style>
</head>
<body>
<div class="container">
  <div class="card">
    <h1>Kite Trading</h1>
    {% if status == 'error' %}
      <div class="status-indicator"><span class="dot red"></span> Error</div>
      <div class="alert">{{ message }}</div>
      <div class="btn-group">
        <a href="/login" class="btn btn-primary">Try Again</a>
        <a href="/dashboard" class="btn btn-secondary">Dashboard</a>
      </div>
    {% else %}
      <div class="status-indicator"><span class="dot blue"></span> Not Authenticated</div>
      <p class="info">Token expired or not yet authenticated</p>
      <div class="btn-group">
        <a href="/login" class="btn btn-primary">Login with Kite</a>
        <a href="/dashboard" class="btn btn-secondary">Dashboard</a>
      </div>
    {% endif %}
  </div>
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
    """Redirect to dashboard if authenticated, otherwise show login page."""
    settings = get_settings()
    token_manager = TokenManager(settings.kite_token_file)
    token = token_manager.load_token()

    if token:
        kite = get_kite()
        kite.set_access_token(token)
        try:
            kite.profile()
            return redirect("/dashboard")
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

        return redirect("/dashboard")

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


LOG_FILE = Path(__file__).parent.parent / "server.log"


_DEBUG_PATHS = ("/api/dashboard", "/status", "/api/logs")


@app.route("/api/logs")
def api_logs():
    """Return last N lines from server.log, with optional level filter."""
    n = request.args.get("lines", 50, type=int)
    n = min(max(n, 1), 500)  # clamp 1-500
    level = request.args.get("level", "info").lower()

    if not LOG_FILE.exists():
        return jsonify({"lines": []})

    try:
        with open(LOG_FILE, "r") as f:
            # Read extra buffer when filtering to ensure enough lines after removal
            buf = deque(f, maxlen=n * 5 if level != "debug" else n)

        lines = [line.rstrip("\n") for line in buf]

        if level != "debug":
            lines = [l for l in lines if not any(p in l for p in _DEBUG_PATHS)]

        return jsonify({"lines": lines[-n:]})
    except Exception as e:
        return jsonify({"error": str(e)}), 500



# Auto-start the strategy engine if it was previously enabled and the client
# has a valid token.  This covers server-restart scenarios.
if _engine.state.settings.enabled and _client.is_authenticated:
    _engine.start()


def run_server(host: str = "0.0.0.0", port: int = 5000, debug: bool = False):
    """Run the web server."""
    app.run(host=host, port=port, debug=debug)


if __name__ == "__main__":
    run_server()
