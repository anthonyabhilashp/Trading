"""Configuration using pydantic-settings."""

from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Kite Connect settings loaded from environment/.env file."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    kite_api_key: str
    kite_api_secret: str
    kite_redirect_url: str = "http://127.0.0.1:5000/callback"
    kite_token_file: Path = Path(__file__).parent.parent / ".kite_tokens.json"


_settings: Settings | None = None


def get_settings() -> Settings:
    """Get cached settings instance."""
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings
