"""Shared test fixtures for MemoryAtlas."""
import sqlite3
import tempfile
from pathlib import Path

import pytest

from memoryatlas.config import Config
from memoryatlas.db import AtlasDB


@pytest.fixture
def tmp_dir(tmp_path):
    """Provide a temporary directory for test data."""
    return tmp_path


@pytest.fixture
def config(tmp_path):
    """Config pointing to temp directories."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    vault_dir = tmp_path / "vault" / "MemoryAtlas"
    vault_dir.mkdir(parents=True)

    return Config(
        data_dir=data_dir,
        db_path=data_dir / "test.db",
        jsonl_path=data_dir / "test.jsonl",
        apple_db_path=tmp_path / "fake_apple.db",
        vault_path=tmp_path / "vault",
        atlas_vault_dir=vault_dir,
    )


@pytest.fixture
def db(config):
    """Initialized test database."""
    database = AtlasDB(config.db_path)
    with database:
        database.init_schema()
        # Add philological columns (migration)
        try:
            database.conn.execute("SELECT restored_text FROM asset LIMIT 1")
        except sqlite3.OperationalError:
            database.conn.execute("ALTER TABLE asset ADD COLUMN restored_text TEXT")
            database.conn.execute("ALTER TABLE asset ADD COLUMN translated_text TEXT")
            database.conn.execute("ALTER TABLE asset ADD COLUMN polished_at TEXT")
            database.conn.commit()
        yield database


@pytest.fixture
def sample_asset(db):
    """Insert a sample asset and return its ID."""
    from memoryatlas.models import Asset

    asset = Asset(
        id="TEST-0001-0001-0001-000000000001",
        source_type="voice_memo",
        source_path="/fake/path/test.m4a",
        filename="test.m4a",
        title="Test Recording",
        duration_sec=120.0,
        recorded_at="2024-01-15T10:30:00Z",
        file_format="m4a",
    )
    db.upsert_asset(asset)
    db.conn.commit()
    return asset


@pytest.fixture
def sample_transcript(tmp_path):
    """Create a sample transcript file."""
    transcript_path = tmp_path / "data" / "transcripts" / "test.txt"
    transcript_path.parent.mkdir(parents=True, exist_ok=True)
    transcript_path.write_text(
        "Привет, это тестовая запись. Мы обсуждаем нейронауку и "
        "философию сознания. Как ты думаешь, consciousness — это "
        "эмерджентное свойство или фундаментальное?",
        encoding="utf-8",
    )
    return str(transcript_path)


@pytest.fixture
def sample_srt(tmp_path):
    """Create a sample SRT file."""
    srt_path = tmp_path / "test.srt"
    srt_path.write_text(
        "1\n"
        "00:00:00,000 --> 00:00:03,000\n"
        "Привет, как дела?\n"
        "\n"
        "2\n"
        "00:00:03,000 --> 00:00:06,000\n"
        "Хорошо, спасибо. А у тебя?\n"
        "\n"
        "3\n"
        "00:00:06,000 --> 00:00:10,000\n"
        "Нормально. Давай обсудим наш проект.\n",
        encoding="utf-8",
    )
    return str(srt_path)
