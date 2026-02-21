"""Generate Obsidian markdown notes from atlas.db assets."""
import hashlib
from pathlib import Path

from .config import Config
from .db import AtlasDB
from .models import Asset
from .util import write_jsonl, row_to_asset


def generate_note_content(asset: Asset) -> str:
    """Generate markdown content for a single asset note."""
    lines = ["---"]
    lines.append(f"uuid: {asset.id}")
    lines.append(f"type: {asset.source_type}")

    title_escaped = (asset.title or "Untitled").replace('"', '\\"')
    lines.append(f'title: "{title_escaped}"')

    if asset.recorded_at:
        lines.append(f"recorded: {asset.recorded_at}")
        lines.append(f"date: {asset.recorded_date}")

    if asset.duration_sec is not None:
        lines.append(f"duration: {asset.duration_display}")
        lines.append(f"duration_sec: {asset.duration_sec:.1f}")

    lines.append(f"format: {asset.file_format or 'unknown'}")
    lines.append(f"transcribed: {str(asset.transcript_status == 'done').lower()}")
    lines.append(f"enriched: {str(asset.summary is not None).lower()}")

    tags = ["memoryatlas", f"memoryatlas/{asset.source_type.replace('_', '-')}"]
    if asset.transcript_status == "done":
        tags.append("memoryatlas/transcribed")
    lines.append(f"tags: [{', '.join(tags)}]")

    lines.append(f"atlas-id: {asset.short_id}")

    if asset.has_gps and asset.lat is not None:
        lines.append(f"location: [{asset.lat}, {asset.lon}]")
        if asset.place:
            lines.append(f"place: \"{asset.place}\"")

    lines.append("---")
    lines.append("")

    display_title = asset.title or "Untitled Recording"
    lines.append(f"# {display_title}")
    lines.append("")

    lines.append("| Field | Value |")
    lines.append("|-------|-------|")
    if asset.recorded_at:
        lines.append(f"| Recorded | {asset.recorded_at} |")
    lines.append(f"| Duration | {asset.duration_display} |")
    lines.append(f"| Format | .{asset.file_format or '?'} |")
    lines.append(f"| Source | Apple Voice Memos |")
    lines.append(f"| UUID | `{asset.short_id}` |")
    if asset.place:
        lines.append(f"| Location | {asset.place} |")
    lines.append("")

    if asset.transcript_status == "done" and asset.transcript_path:
        lines.append("## Transcript")
        lines.append("")
        lines.append(f"[Open transcript](file://{asset.transcript_path})")
        lines.append("")
    else:
        lines.append("## Transcript")
        lines.append("")
        lines.append("*Pending transcription.*")
        lines.append("")

    if asset.summary:
        lines.append("## Summary")
        lines.append("")
        lines.append(asset.summary)
        lines.append("")

        # Add enrichment metadata if available
        if asset.topics:
            lines.append(f"**Topics**: {asset.topics}")
            lines.append("")
        if asset.people and asset.people.lower() != "none":
            lines.append(f"**People**: {asset.people}")
            lines.append("")
        if asset.sentiment:
            lines.append(f"**Sentiment**: {asset.sentiment}")
            lines.append("")

    return "\n".join(lines)


def publish(config: Config, db: AtlasDB, verbose: bool = False,
            force: bool = False) -> dict:
    """Generate Obsidian notes for assets."""
    counts = {"created": 0, "updated": 0, "skipped": 0, "error": 0}

    db.log_action("publish", "start")

    voice_dir = config.atlas_vault_dir / "voice"
    voice_dir.mkdir(parents=True, exist_ok=True)

    rows = db.get_all_assets() if force else db.get_unpublished_assets()

    for row in rows:
        try:
            asset = row_to_asset(row)
            content = generate_note_content(asset)
            content_hash = hashlib.sha256(content.encode()).hexdigest()[:16]

            note_filename = asset.note_filename
            note_path = voice_dir / note_filename
            relative_note_path = f"MemoryAtlas/voice/{note_filename}"

            if note_path.exists() and asset.note_hash == content_hash and not force:
                counts["skipped"] += 1
                continue

            is_new = not note_path.exists()
            note_path.write_text(content, encoding="utf-8")

            db.mark_published(asset.id, relative_note_path, content_hash)
            action = "create" if is_new else "update"
            db.log_action("publish", action, asset_id=asset.id, detail={
                "note_path": relative_note_path,
            })
            write_jsonl(config.jsonl_path, "publish", action, asset.id, {
                "note_path": relative_note_path,
            })

            if is_new:
                counts["created"] += 1
            else:
                counts["updated"] += 1

            if verbose:
                print(f"  {action}: {note_filename}")

        except Exception as e:
            counts["error"] += 1
            db.log_action("publish", "error",
                         asset_id=row["id"] if row else None,
                         detail={"error": str(e)})
            if verbose:
                print(f"  ERROR: {e}")

    db.conn.commit()
    db.log_action("publish", "complete", detail=counts)
    db.conn.commit()

    return counts


def publish_index(config: Config, db: AtlasDB):
    """Write/update the _Index.md Dataview dashboard."""
    stats = db.get_stats()

    content = f"""---
tags: [memoryatlas, memoryatlas/index]
---

# MemoryAtlas

> {stats['total']} recordings | {stats['total_hours']:.0f} hours | {stats['published']} published | {stats['transcribed']} transcribed

## Recent Recordings

```dataview
TABLE duration, recorded, transcribed
FROM "MemoryAtlas/voice"
WHERE type = "voice_memo"
SORT recorded DESC
LIMIT 20
```

## Longest Recordings

```dataview
TABLE duration, duration_sec, recorded, title
FROM "MemoryAtlas/voice"
SORT duration_sec DESC
LIMIT 10
```

## By Year

```dataview
TABLE length(rows) AS Count, round(sum(rows.duration_sec) / 3600, 1) AS Hours
FROM "MemoryAtlas/voice"
GROUP BY dateformat(date(recorded), "yyyy") AS Year
SORT Year DESC
```

## Pending Transcription

```dataview
TABLE duration, recorded
FROM "MemoryAtlas/voice"
WHERE transcribed = false
SORT duration_sec ASC
LIMIT 20
```
"""
    index_path = config.atlas_vault_dir / "_Index.md"
    config.atlas_vault_dir.mkdir(parents=True, exist_ok=True)
    index_path.write_text(content, encoding="utf-8")


def publish_about(config: Config):
    """Write the _About.md system documentation note."""
    content = """---
tags: [memoryatlas, memoryatlas/system]
---

# About MemoryAtlas

MemoryAtlas is a local-first pipeline that ingests voice memos (and later videos, photos, location data), transcribes them with Whisper, and publishes lightweight notes into this Obsidian vault. Everything runs on your Mac Studio — no cloud, no data leaves your machine.

## How It Works

1. **Scan**: Reads Apple Voice Memos metadata from CloudRecordings.db (read-only, never modifies originals)
2. **Publish**: Creates one markdown note per recording with frontmatter for Dataview queries
3. **Transcribe** (Phase 2): Runs OpenAI Whisper locally for EN/RU bilingual transcription
4. **Enrich** (Phase 4): Joins Google/Apple location history, LLM summaries via Ollama

## Where Things Live

| What | Where | Why |
|------|-------|-----|
| These notes | `MemoryAtlas/` in PracticeLife vault | Lightweight, searchable |
| Heavy data | `~/tools/memoryatlas/data/` | Keeps Obsidian fast |
| Transcripts | `~/tools/memoryatlas/data/transcripts/` | Too large for vault |
| Source audio | Apple Voice Memos (untouched) | Read-only, never modified |
| Database | `~/tools/memoryatlas/data/atlas.db` | Source of truth |
| Action log | `~/tools/memoryatlas/data/atlas.jsonl` | Append-only audit trail |

## Privacy Model

- All processing is local (Mac Studio M2 Max)
- Whisper runs on-device, no API calls
- Ollama enrichment runs on-device
- No audio files are copied into the vault
- Source Voice Memos database is opened read-only (immutable mode)

## Commands

```bash
source ~/tools/memoryatlas/.venv/bin/activate
atlas init       # Initialize database and vault folders
atlas scan       # Import voice memo metadata
atlas publish    # Generate Obsidian notes
atlas status     # Show statistics
atlas doctor     # Check system health
atlas info UUID  # Show details for one asset
```

## Frontmatter Fields (Dataview)

- `uuid` / `atlas-id`: Recording identifier
- `type`: voice_memo (later: video, audio_import)
- `title`, `date`, `recorded`: Metadata from Voice Memos
- `duration` / `duration_sec`: Human-readable and raw seconds
- `transcribed` / `enriched`: Boolean status flags
- `location` / `place`: GPS coordinates and place name (Phase 4)
- `tags`: Always includes `memoryatlas`
"""
    about_path = config.atlas_vault_dir / "_About.md"
    config.atlas_vault_dir.mkdir(parents=True, exist_ok=True)
    about_path.write_text(content, encoding="utf-8")
