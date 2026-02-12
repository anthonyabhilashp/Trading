#!/usr/bin/env python3
"""Generate and store Kite Connect access token."""

import sys
from pathlib import Path

# Add parent dir to path for direct script execution
sys.path.insert(0, str(Path(__file__).parent.parent))

from kite_wrapper import KiteClient


def main():
    """Run the token generation flow."""
    print("Kite Connect Token Generator")
    print("=" * 40)

    client = KiteClient()

    if client.is_authenticated:
        print(f"Existing valid token found.")
        try:
            profile = client.kite.profile()
            print(f"Logged in as: {profile['user_name']} ({profile['user_id']})")

            response = input("\nGenerate new token anyway? [y/N]: ")
            if response.lower() != "y":
                print("Using existing token.")
                return
        except Exception as e:
            print(f"Existing token invalid: {e}")
            print("Generating new token...")

    try:
        client.login()
        profile = client.kite.profile()
        print(f"\nSuccess! Logged in as: {profile['user_name']}")
        print(f"Token saved to: {client.settings.kite_token_file}")
    except Exception as e:
        print(f"\nError: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
