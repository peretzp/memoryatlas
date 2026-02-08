"""Scan sources and upsert assets into atlas.db."""
from .config import Config
from .db import AtlasDB
from .apple import read_voice_memos
from .util import write_jsonl


def scan(config: Config, db: AtlasDB, verbose: bool = False) -> dict:
    """
    Scan Apple Voice Memos and upsert into atlas.db.

    Reads Apple's CloudRecordings.db (read-only, immutable) and upserts
    each recording into our atlas.db. Idempotent: reruns produce no
    duplicates and skip unchanged assets.
    """
    counts = {"insert": 0, "update": 0, "skip": 0, "error": 0}

    db.log_action("scan", "start", detail={"source": "voice_memos"})

    if config.scan_voice_memos:
        assets = read_voice_memos(config.apple_db_path)

        for asset in assets:
            try:
                result = db.upsert_asset(asset)
                counts[result] += 1

                if result != "skip":
                    db.log_action("scan", result, asset_id=asset.id, detail={
                        "title": asset.title,
                        "duration_sec": asset.duration_sec,
                        "recorded_at": asset.recorded_at,
                    })
                    write_jsonl(config.jsonl_path, "scan", result, asset.id, {
                        "title": asset.title,
                    })

                    if verbose:
                        print(f"  {result}: {asset.title or asset.filename} ({asset.duration_display})")
            except Exception as e:
                counts["error"] += 1
                db.log_action("scan", "error", asset_id=asset.id, detail={"error": str(e)})
                if verbose:
                    print(f"  ERROR: {asset.id}: {e}")

    db.conn.commit()
    db.log_action("scan", "complete", detail=counts)
    db.conn.commit()

    return counts
