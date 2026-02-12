"""Flask server to capture OAuth callback."""

import threading
import webbrowser
from dataclasses import dataclass

from flask import Flask, request


@dataclass
class AuthResult:
    """Result from OAuth callback."""

    request_token: str | None = None
    error: str | None = None


def run_auth_server(login_url: str, port: int = 5000) -> AuthResult:
    """
    Run a temporary Flask server to capture the OAuth callback.
    Opens browser to login URL and waits for callback.
    Returns the request_token from the callback.
    """
    app = Flask(__name__)
    app.logger.disabled = True

    import logging
    log = logging.getLogger("werkzeug")
    log.setLevel(logging.ERROR)

    result = AuthResult()
    shutdown_event = threading.Event()

    @app.route("/callback")
    def callback():
        result.request_token = request.args.get("request_token")
        result.error = request.args.get("error")
        shutdown_event.set()

        if result.error:
            return f"<h2>Authentication Failed</h2><p>{result.error}</p>"
        return "<h2>Authentication Successful</h2><p>You can close this window.</p>"

    def run_server():
        app.run(port=port, debug=False, use_reloader=False)

    server_thread = threading.Thread(target=run_server, daemon=True)
    server_thread.start()

    print(f"Opening browser for login...")
    webbrowser.open(login_url)

    print("Waiting for authentication callback...")
    shutdown_event.wait(timeout=300)  # 5 min timeout

    if not result.request_token and not result.error:
        result.error = "Timeout waiting for callback"

    return result
