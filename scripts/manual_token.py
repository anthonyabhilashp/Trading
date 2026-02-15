"""Exchange a request token manually (when browser callback can't reach the server)."""

import sys
from kite_wrapper import KiteClient


def main():
    if len(sys.argv) < 2:
        print("Usage: python -m scripts.manual_token <request_token>")
        print("  Get request_token from the callback URL parameter")
        sys.exit(1)

    request_token = sys.argv[1]
    client = KiteClient()

    data = client.kite.generate_session(
        request_token,
        api_secret=client.settings.kite_api_secret,
    )

    access_token = data["access_token"]
    user_id = data.get("user_id", "")

    client.kite.set_access_token(access_token)
    client.token_manager.save_token(access_token, user_id)

    print(f"Logged in as: {user_id}")
    print("Token saved. You're good to go.")


if __name__ == "__main__":
    main()
