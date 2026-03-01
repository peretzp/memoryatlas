"""Tests for database operations."""
from memoryatlas.models import Asset
from memoryatlas.db import AtlasDB


def test_init_schema(db):
    """Schema creates required tables."""
    tables = db.conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    names = [t["name"] for t in tables]
    assert "asset" in names
    assert "action_log" in names
    assert "schema_version" in names


def test_upsert_insert(db):
    asset = Asset(id="NEW-001", source_type="voice_memo",
                  source_path="/test.m4a", filename="test.m4a",
                  title="New Recording", duration_sec=60.0)
    result = db.upsert_asset(asset)
    db.conn.commit()
    assert result == "insert"

    row = db.get_asset("NEW-001")
    assert row is not None
    assert row["title"] == "New Recording"


def test_upsert_skip(db):
    asset = Asset(id="SKIP-001", source_type="voice_memo",
                  source_path="/test.m4a", filename="test.m4a",
                  title="Same", duration_sec=60.0, recorded_at="2024-01-01T00:00:00Z")
    db.upsert_asset(asset)
    db.conn.commit()

    result = db.upsert_asset(asset)
    assert result == "skip"


def test_upsert_update(db):
    asset = Asset(id="UPD-001", source_type="voice_memo",
                  source_path="/test.m4a", filename="test.m4a",
                  title="Original", duration_sec=60.0)
    db.upsert_asset(asset)
    db.conn.commit()

    asset.title = "Updated"
    result = db.upsert_asset(asset)
    db.conn.commit()
    assert result == "update"

    row = db.get_asset("UPD-001")
    assert row["title"] == "Updated"


def test_log_action(db):
    db.log_action("test", "test_action", asset_id="A1", detail={"key": "value"})
    db.conn.commit()

    row = db.conn.execute("SELECT * FROM action_log WHERE command = 'test'").fetchone()
    assert row is not None
    assert row["action"] == "test_action"
    assert row["asset_id"] == "A1"


def test_mark_published(db, sample_asset):
    db.mark_published(sample_asset.id, "MemoryAtlas/voice/test.md", "abc123")
    db.conn.commit()

    row = db.get_asset(sample_asset.id)
    assert row["note_path"] == "MemoryAtlas/voice/test.md"
    assert row["note_hash"] == "abc123"
    assert row["published_at"] is not None


def test_get_stats(db, sample_asset):
    stats = db.get_stats()
    assert stats["total"] == 1
    assert stats["published"] == 0
    assert stats["transcribed"] == 0


def test_get_unpublished(db, sample_asset):
    rows = db.get_unpublished_assets()
    assert len(rows) == 1
    assert rows[0]["id"] == sample_asset.id


def test_get_all_assets(db, sample_asset):
    rows = db.get_all_assets()
    assert len(rows) == 1


def test_wal_mode(db):
    mode = db.conn.execute("PRAGMA journal_mode").fetchone()
    assert mode[0] == "wal"
