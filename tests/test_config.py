"""Tests for configuration management."""
from pathlib import Path

from memoryatlas.config import Config


def test_default_config():
    config = Config()
    assert config.data_dir == Path.home() / "tools" / "memoryatlas" / "data"
    assert config.scan_voice_memos is True
    assert config.ollama_model == "qwen2.5:32b"


def test_config_from_yaml(tmp_path):
    yaml_file = tmp_path / "config.yaml"
    yaml_file.write_text(
        "data_dir: ~/test_data\n"
        "ollama_model: llama3.3:70b\n"
        "scan_voice_memos: false\n"
    )
    config = Config.load(yaml_file)
    assert config.data_dir == Path.home() / "test_data"
    assert config.ollama_model == "llama3.3:70b"
    assert config.scan_voice_memos is False


def test_config_missing_file():
    config = Config.load(Path("/nonexistent/config.yaml"))
    # Should return defaults when file doesn't exist
    assert config.data_dir == Path.home() / "tools" / "memoryatlas" / "data"


def test_config_empty_yaml(tmp_path):
    yaml_file = tmp_path / "config.yaml"
    yaml_file.write_text("")
    config = Config.load(yaml_file)
    assert config.data_dir == Path.home() / "tools" / "memoryatlas" / "data"


def test_config_partial_yaml(tmp_path):
    yaml_file = tmp_path / "config.yaml"
    yaml_file.write_text("ollama_model: custom:7b\n")
    config = Config.load(yaml_file)
    assert config.ollama_model == "custom:7b"
    # Other fields should have defaults
    assert config.scan_voice_memos is True


def test_config_tilde_expansion(tmp_path):
    yaml_file = tmp_path / "config.yaml"
    yaml_file.write_text("data_dir: ~/my_atlas_data\n")
    config = Config.load(yaml_file)
    assert str(config.data_dir).startswith(str(Path.home()))
    assert str(config.data_dir).endswith("my_atlas_data")
