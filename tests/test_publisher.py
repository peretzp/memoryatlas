"""Tests for Obsidian note generation."""
from memoryatlas.models import Asset
from memoryatlas.publisher import generate_note_content


def test_basic_note_content():
    asset = Asset(
        id="TEST-001", source_type="voice_memo",
        source_path="/t.m4a", filename="t.m4a",
        title="Test Note", duration_sec=120.0,
        recorded_at="2024-01-15T10:30:00Z",
        file_format="m4a",
    )
    content = generate_note_content(asset)

    assert "---" in content  # frontmatter
    assert 'title: "Test Note"' in content
    assert "duration: 2:00" in content
    assert "# Test Note" in content
    assert "transcribed: false" in content
    assert "polished: false" in content


def test_note_with_enrichment():
    asset = Asset(
        id="TEST-002", source_type="voice_memo",
        source_path="/t.m4a", filename="t.m4a",
        title="Enriched Note",
        summary="A discussion about philosophy.",
        topics="philosophy, consciousness",
        people="Dad, Sofia",
        sentiment="positive",
    )
    content = generate_note_content(asset)

    assert "## Summary" in content
    assert "A discussion about philosophy." in content
    assert "**Topics**: philosophy, consciousness" in content
    assert "**People**: Dad, Sofia" in content
    assert "**Sentiment**: positive" in content
    assert "enriched: true" in content


def test_note_with_translation():
    asset = Asset(
        id="TEST-003", source_type="voice_memo",
        source_path="/t.m4a", filename="t.m4a",
        title="Polished Note",
        restored_text="Восстановленный текст.",
        translated_text="Translated text with literary quality.",
        polished_at="2024-01-15T12:00:00Z",
    )
    content = generate_note_content(asset)

    assert "## Translation" in content
    assert "Translated text with literary quality." in content
    assert "## Restored Transcript" in content
    assert "Восстановленный текст." in content
    assert "polished: true" in content


def test_note_translation_before_restored():
    """Translation should appear before restored transcript in note."""
    asset = Asset(
        id="TEST-004", source_type="voice_memo",
        source_path="/t.m4a", filename="t.m4a",
        title="Order Test",
        restored_text="Русский текст.",
        translated_text="English text.",
        polished_at="2024-01-15T12:00:00Z",
    )
    content = generate_note_content(asset)

    translation_pos = content.index("## Translation")
    restored_pos = content.index("## Restored Transcript")
    assert translation_pos < restored_pos


def test_note_people_none_excluded():
    """'None' people should not show up."""
    asset = Asset(
        id="TEST-005", source_type="voice_memo",
        source_path="/t.m4a", filename="t.m4a",
        title="No People",
        summary="A solo recording.",
        people="none",
    )
    content = generate_note_content(asset)
    assert "**People**" not in content


def test_note_with_location():
    asset = Asset(
        id="TEST-006", source_type="voice_memo",
        source_path="/t.m4a", filename="t.m4a",
        title="Located Note",
        has_gps=1, lat=37.7749, lon=-122.4194,
        place="San Francisco, CA",
    )
    content = generate_note_content(asset)

    assert "location: [37.7749, -122.4194]" in content
    assert 'place: "San Francisco, CA"' in content
    assert "| Location | San Francisco, CA |" in content
