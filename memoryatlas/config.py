"""Configuration management."""
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from .constants import (
    DEFAULT_DATA_DIR, DEFAULT_DB_PATH, DEFAULT_JSONL_PATH,
    APPLE_VOICEMEMOS_DB, OBSIDIAN_VAULT, MEMORYATLAS_VAULT_DIR,
)


@dataclass
class Config:
    data_dir: Path = DEFAULT_DATA_DIR
    db_path: Path = DEFAULT_DB_PATH
    jsonl_path: Path = DEFAULT_JSONL_PATH
    apple_db_path: Path = APPLE_VOICEMEMOS_DB
    vault_path: Path = OBSIDIAN_VAULT
    atlas_vault_dir: Path = MEMORYATLAS_VAULT_DIR
    scan_voice_memos: bool = True
    publish_unpublished_only: bool = True
    overwrite_notes: bool = False
    whisper_env: Path = Path.home() / "tools" / "whisper-env"
    whisper_model: str = "turbo"
    whisper_language: Optional[str] = None
    ollama_model: str = "qwen2.5:32b"

    @classmethod
    def load(cls, path: Optional[Path] = None) -> "Config":
        config_path = path or (Path.home() / "tools" / "memoryatlas" / "config.yaml")
        if config_path.exists():
            import yaml
            with open(config_path) as f:
                raw = yaml.safe_load(f) or {}
            for key in ("data_dir", "db_path", "jsonl_path", "apple_db_path",
                        "vault_path", "atlas_vault_dir", "whisper_env"):
                if key in raw:
                    raw[key] = Path(raw[key]).expanduser()
            return cls(**{k: v for k, v in raw.items() if hasattr(cls, k)})
        return cls()
