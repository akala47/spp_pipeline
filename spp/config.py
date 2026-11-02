"""Runtime configuration.

Everything is environment-driven so the same code runs from a laptop, from
Airflow, and from a container without a config file per environment.

    SPP_DSN            postgresql://user:pass@host:5432/spp   (required to write)
    SPP_GROUPS_FILE    override path to config/groups.json
    SPP_LOG_LEVEL      INFO (default) | DEBUG | WARNING
    SPP_LOG_FORMAT     json (default) | text
    SPP_HTTP_TIMEOUT   seconds, default 30
    SPP_<FEED>_GROUPS  per-feed group override, e.g. SPP_RT_LMP_GROUPS=nppd,kmea
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from dotenv import load_dotenv

# Read the repo's .env (found by searching up from this file, so it works from
# any working directory). Variables already set in the environment win, which
# is how Airflow / Astro passes them in.
load_dotenv()

# Base of the SPP file browser. Feed URLs append `/<dataset>?path=...`.
SPP_PORTAL = os.environ.get("SPP_PORTAL", "https://portal.spp.org/file-browser-api/download")
REPO_ROOT = Path(__file__).resolve().parent.parent


class MissingConfiguration(RuntimeError):
    """Raised when something required at runtime was never set."""


@dataclass(frozen=True)
class Settings:
    @property
    def dsn(self) -> str | None:
        return os.environ.get("SPP_DSN")

    def require_dsn(self) -> str:
        dsn = self.dsn
        if not dsn:
            raise MissingConfiguration(
                "SPP_DSN is not set. Export a Postgres DSN, e.g.\n"
                "  export SPP_DSN=postgresql://user:pass@host:5432/spp"
            )
        return dsn

    @property
    def groups_file(self) -> Path:
        override = os.environ.get("SPP_GROUPS_FILE")
        return Path(override) if override else REPO_ROOT / "config" / "groups.json"

    @property
    def log_level(self) -> str:
        return os.environ.get("SPP_LOG_LEVEL", "INFO").upper()

    @property
    def log_format(self) -> str:
        return os.environ.get("SPP_LOG_FORMAT", "json").lower()

    @property
    def http_timeout(self) -> float:
        return float(os.environ.get("SPP_HTTP_TIMEOUT", "30"))

    @property
    def http_retries(self) -> int:
        return int(os.environ.get("SPP_HTTP_RETRIES", "3"))


settings = Settings()
