"""Immutable constants for MemoryAtlas."""
from pathlib import Path

VERSION = "0.1.0"

# Apple epoch: 2001-01-01T00:00:00Z in Unix time
APPLE_EPOCH_OFFSET = 978307200

# Default paths
DEFAULT_PROJECT_DIR = Path.home() / "tools" / "memoryatlas"
DEFAULT_DATA_DIR = DEFAULT_PROJECT_DIR / "data"
DEFAULT_DB_PATH = DEFAULT_DATA_DIR / "atlas.db"
DEFAULT_JSONL_PATH = DEFAULT_DATA_DIR / "atlas.jsonl"
DEFAULT_TRANSCRIPTS_DIR = DEFAULT_DATA_DIR / "transcripts"
DEFAULT_CACHE_DIR = DEFAULT_DATA_DIR / "cache"

APPLE_VOICEMEMOS_DB = (
    Path.home()
    / "Library"
    / "Group Containers"
    / "group.com.apple.VoiceMemos.shared"
    / "Recordings"
    / "CloudRecordings.db"
)

APPLE_VOICEMEMOS_DIR = APPLE_VOICEMEMOS_DB.parent

OBSIDIAN_VAULT = (
    Path.home()
    / "Library"
    / "Mobile Documents"
    / "iCloud~md~obsidian"
    / "Documents"
    / "PracticeLife"
)

MEMORYATLAS_VAULT_DIR = OBSIDIAN_VAULT / "MemoryAtlas"
MEMORYATLAS_VOICE_DIR = MEMORYATLAS_VAULT_DIR / "voice"

# File extensions
AUDIO_EXTENSIONS = {".m4a", ".qta", ".mp3", ".wav", ".aac", ".ogg", ".flac"}
VIDEO_EXTENSIONS = {".mp4", ".mov", ".avi", ".mkv", ".webm"}
