"""Tests for philological pipeline.

Uses mocking for LLM calls — no actual Ollama/Claude needed.
"""
from unittest.mock import patch, MagicMock

import pytest

from memoryatlas.philologist import (
    pre_clean,
    parse_srt,
    write_srt,
    _reattach_timing,
    SPAM_PATTERNS,
    LANG_MAP,
    check_ollama_health,
)


# ---------------------------------------------------------------------------
# pre_clean tests
# ---------------------------------------------------------------------------

def test_pre_clean_removes_subtitle_spam():
    text = "Привет " + "Субтитры сделал DimaTorzok " * 50 + "мир"
    cleaned = pre_clean(text)
    assert "DimaTorzok" not in cleaned
    assert "Привет" in cleaned
    assert "мир" in cleaned


def test_pre_clean_removes_ugu_spam():
    text = "Начало. " + "Угу. " * 30 + "Конец."
    cleaned = pre_clean(text)
    # Should be reduced (hallucination removed marker)
    assert cleaned.count("Угу") < 10


def test_pre_clean_removes_stron_spam():
    text = "Text " + "stron " * 20 + "more text"
    cleaned = pre_clean(text)
    assert "stron stron" not in cleaned


def test_pre_clean_preserves_normal_text():
    text = "Это нормальный текст без артефактов. Всё хорошо."
    cleaned = pre_clean(text)
    assert cleaned == text


def test_pre_clean_collapses_whitespace():
    text = "Line 1\n\n\n\n\n\n\n\nLine 2"
    cleaned = pre_clean(text)
    assert "\n\n\n\n" not in cleaned


# ---------------------------------------------------------------------------
# SRT parsing tests
# ---------------------------------------------------------------------------

def test_parse_srt_basic(sample_srt):
    from pathlib import Path
    text = Path(sample_srt).read_text()
    entries = parse_srt(text)

    assert len(entries) == 3
    assert entries[0]["index"] == 1
    assert entries[0]["start"] == "00:00:00,000"
    assert entries[0]["end"] == "00:00:03,000"
    assert entries[0]["text"] == "Привет, как дела?"


def test_parse_srt_empty():
    entries = parse_srt("")
    assert entries == []


def test_parse_srt_malformed():
    text = "Not a valid SRT file\nJust some text\n"
    entries = parse_srt(text)
    assert entries == []


def test_parse_srt_with_multiline():
    text = (
        "1\n"
        "00:00:00,000 --> 00:00:03,000\n"
        "Line one\n"
        "Line two\n"
        "\n"
    )
    entries = parse_srt(text)
    assert len(entries) == 1
    assert entries[0]["text"] == "Line one\nLine two"


def test_write_srt_roundtrip(sample_srt):
    from pathlib import Path
    text = Path(sample_srt).read_text()
    entries = parse_srt(text)
    output = write_srt(entries)
    re_parsed = parse_srt(output)

    assert len(re_parsed) == len(entries)
    for orig, reparsed in zip(entries, re_parsed):
        assert orig["start"] == reparsed["start"]
        assert orig["end"] == reparsed["end"]
        assert orig["text"] == reparsed["text"]


# ---------------------------------------------------------------------------
# Timing reattachment tests
# ---------------------------------------------------------------------------

def test_reattach_timing_basic():
    original = [
        {"index": 1, "start": "00:00:00,000", "end": "00:00:03,000", "text": "Привет"},
        {"index": 2, "start": "00:00:03,000", "end": "00:00:06,000", "text": "Мир"},
    ]
    llm_output = "[1] Hello\n[2] World"

    result = _reattach_timing(llm_output, original)
    assert result[0]["text"] == "Hello"
    assert result[0]["start"] == "00:00:00,000"
    assert result[1]["text"] == "World"
    assert result[1]["start"] == "00:00:03,000"


def test_reattach_timing_missing_marker():
    """Unmatched entries should keep original text."""
    original = [
        {"index": 1, "start": "00:00:00,000", "end": "00:00:03,000", "text": "Original"},
        {"index": 2, "start": "00:00:03,000", "end": "00:00:06,000", "text": "Also original"},
    ]
    llm_output = "[1] Translated first"

    result = _reattach_timing(llm_output, original)
    assert result[0]["text"] == "Translated first"
    assert result[1]["text"] == "Also original"  # Kept original


def test_reattach_timing_multiline():
    original = [
        {"index": 1, "start": "00:00:00,000", "end": "00:00:05,000", "text": "Orig"},
    ]
    llm_output = "[1] First line\nSecond line\nThird line"

    result = _reattach_timing(llm_output, original)
    assert "First line" in result[0]["text"]
    assert "Third line" in result[0]["text"]


# ---------------------------------------------------------------------------
# LANG_MAP coverage
# ---------------------------------------------------------------------------

def test_lang_map_has_russian():
    assert LANG_MAP["ru"] == "Russian"


def test_lang_map_has_common_languages():
    assert "ja" in LANG_MAP
    assert "ko" in LANG_MAP
    assert "tr" in LANG_MAP
    assert "de" in LANG_MAP
    assert "fr" in LANG_MAP


# ---------------------------------------------------------------------------
# Health check (mocked)
# ---------------------------------------------------------------------------

def test_health_check_healthy():
    mock_response = MagicMock()
    mock_response.read.return_value = b'{"models": [{"name": "qwen2.5:32b"}, {"name": "llama3.3:70b"}]}'
    mock_response.__enter__ = lambda s: s
    mock_response.__exit__ = MagicMock(return_value=False)

    with patch("urllib.request.urlopen", return_value=mock_response):
        result = check_ollama_health("qwen2.5:32b")
        assert result["healthy"] is True
        assert result["model_available"] is True
        assert len(result["models_loaded"]) == 2


def test_health_check_model_missing():
    mock_response = MagicMock()
    mock_response.read.return_value = b'{"models": [{"name": "llama3.3:70b"}]}'
    mock_response.__enter__ = lambda s: s
    mock_response.__exit__ = MagicMock(return_value=False)

    with patch("urllib.request.urlopen", return_value=mock_response):
        result = check_ollama_health("qwen2.5:32b")
        assert result["healthy"] is True
        assert result["model_available"] is False


def test_health_check_ollama_down():
    import urllib.error
    with patch("urllib.request.urlopen", side_effect=urllib.error.URLError("Connection refused")):
        result = check_ollama_health()
        assert result["healthy"] is False
        assert "not responding" in result["detail"]


# ---------------------------------------------------------------------------
# LLM call tests (mocked)
# ---------------------------------------------------------------------------

def test_call_ollama_api_success():
    from memoryatlas.philologist import call_ollama_api

    mock_response = MagicMock()
    mock_response.read.return_value = b'{"response": "Hello world"}'
    mock_response.__enter__ = lambda s: s
    mock_response.__exit__ = MagicMock(return_value=False)

    with patch("urllib.request.urlopen", return_value=mock_response):
        result = call_ollama_api("test prompt")
        assert result == "Hello world"


def test_call_ollama_api_retry_on_connection_refused():
    """Should retry on transient errors."""
    from memoryatlas.philologist import call_ollama_api
    import urllib.error

    mock_response = MagicMock()
    mock_response.read.return_value = b'{"response": "success after retry"}'
    mock_response.__enter__ = lambda s: s
    mock_response.__exit__ = MagicMock(return_value=False)

    # Fail first, succeed second
    with patch("urllib.request.urlopen",
               side_effect=[
                   urllib.error.URLError("Connection refused"),
                   mock_response,
               ]):
        with patch("time.sleep"):  # Skip actual sleep
            result = call_ollama_api("test", max_retries=2, base_delay=0.01)
            assert result == "success after retry"


def test_call_claude_api_no_key():
    from memoryatlas.philologist import call_claude_api
    import os

    with patch.dict(os.environ, {}, clear=True):
        result = call_claude_api("test prompt")
        assert result is None


def test_call_llm_routes_to_ollama():
    from memoryatlas.philologist import call_llm

    with patch("memoryatlas.philologist.call_ollama_api", return_value="ollama result") as mock:
        result = call_llm("test", backend="ollama")
        assert result == "ollama result"
        mock.assert_called_once()


def test_call_llm_routes_to_claude():
    from memoryatlas.philologist import call_llm

    with patch("memoryatlas.philologist.call_claude_api", return_value="claude result") as mock:
        result = call_llm("test", backend="claude")
        assert result == "claude result"
        mock.assert_called_once()


def test_call_llm_auto_falls_back():
    from memoryatlas.philologist import call_llm

    with patch("memoryatlas.philologist.call_claude_api", return_value=None):
        with patch("memoryatlas.philologist.call_ollama_api", return_value="ollama fallback") as mock:
            result = call_llm("test", backend="auto")
            assert result == "ollama fallback"
            mock.assert_called_once()
