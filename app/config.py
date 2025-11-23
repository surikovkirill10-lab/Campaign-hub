from __future__ import annotations
import pathlib, yaml
from functools import lru_cache
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

class SystemAuth(BaseModel):
    type: str = "basic"      # basic|bearer|headers|cookie
    username: str | None = None
    password: str | None = None
    token: str | None = None
    headers: dict[str, str] = Field(default_factory=dict)
    cookies: dict[str, str] = Field(default_factory=dict)

class SystemSettings(BaseModel):
    base_url: str = ""
    connect_url: str | None = None
    auth: SystemAuth = Field(default_factory=SystemAuth)

class IMAPSettings(BaseModel):
    host: str = "imap.yandex.ru"
    port: int = 993
    user: str = ""
    password: str = ""
    mailbox: str = "INBOX"
    two_factor: str = "none"  # none|app_password

class SchedulerSettings(BaseModel):
    interval_minutes: int = 60

class Settings(BaseSettings):
    system: SystemSettings = Field(default_factory=SystemSettings)
    imap: IMAPSettings = Field(default_factory=IMAPSettings)
    scheduler: SchedulerSettings = Field(default_factory=SchedulerSettings)
    default_currency: str = "RUB"

    model_config = SettingsConfigDict(env_prefix="", env_nested_delimiter="__")

def load_yaml_config() -> dict:
    here = pathlib.Path(__file__).resolve().parent.parent
    config_path = here / ".." / "config.yaml"
    if config_path.is_file():
        with config_path.open("r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    return {}

@lru_cache(maxsize=1)
def get_settings() -> Settings:
    initial = load_yaml_config()
    return Settings.model_validate(initial)
