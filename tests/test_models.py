"""Tests for data models."""
from memoryatlas.models import Asset


def test_asset_short_id():
    asset = Asset(id="ABC12345-1234-5678-ABCD-123456789012",
                  source_type="voice_memo", source_path="/test", filename="test.m4a")
    assert asset.short_id == "ABC12345"


def test_asset_duration_display_minutes():
    asset = Asset(id="test", source_type="voice_memo", source_path="/t", filename="t.m4a",
                  duration_sec=185.5)
    assert asset.duration_display == "3:05"


def test_asset_duration_display_hours():
    asset = Asset(id="test", source_type="voice_memo", source_path="/t", filename="t.m4a",
                  duration_sec=7261.0)
    assert asset.duration_display == "2:01:01"


def test_asset_duration_display_none():
    asset = Asset(id="test", source_type="voice_memo", source_path="/t", filename="t.m4a")
    assert asset.duration_display == "unknown"


def test_asset_recorded_date():
    asset = Asset(id="test", source_type="voice_memo", source_path="/t", filename="t.m4a",
                  recorded_at="2024-01-15T10:30:00Z")
    assert asset.recorded_date == "2024-01-15"


def test_asset_slug_title():
    asset = Asset(id="test", source_type="voice_memo", source_path="/t", filename="t.m4a",
                  title="My Test Recording! (part 2)")
    assert asset.slug_title == "my-test-recording-part-2"


def test_asset_slug_title_none():
    asset = Asset(id="test", source_type="voice_memo", source_path="/t", filename="t.m4a")
    assert asset.slug_title == "untitled"


def test_asset_note_filename():
    asset = Asset(id="ABC12345-1234-5678-ABCD-123456789012",
                  source_type="voice_memo", source_path="/t", filename="t.m4a",
                  title="Dad", recorded_at="2024-01-15T10:30:00Z")
    assert asset.note_filename == "2024-01-15_dad_ABC12345.md"


def test_asset_polished_fields():
    asset = Asset(id="test", source_type="voice_memo", source_path="/t", filename="t.m4a",
                  restored_text="Restored text", translated_text="Translated text",
                  polished_at="2024-01-15T10:30:00Z")
    assert asset.restored_text == "Restored text"
    assert asset.translated_text == "Translated text"
    assert asset.polished_at is not None
