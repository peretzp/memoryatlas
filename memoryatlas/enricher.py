"""Enrich transcripts with AI-generated metadata: summary, topics, people, sentiment.

Uses local Ollama for LLM inference (privacy-preserving, no API costs).
"""
import json
import time
import subprocess
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional

from .config import Config
from .db import AtlasDB


ENRICHMENT_PROMPT = """Analyze this voice memo transcript and extract:

1. **Summary**: 2-3 sentence summary of the main content
2. **Topics**: List of key topics/themes (comma-separated, max 5)
3. **People**: Names of people mentioned (comma-separated, or "none" if none)
4. **Sentiment**: Overall emotional tone (positive/negative/neutral/mixed)

Transcript:
{transcript}

Respond ONLY with valid JSON in this exact format:
{{
  "summary": "...",
  "topics": "topic1, topic2, topic3",
  "people": "Person Name, Another Person",
  "sentiment": "positive"
}}"""


def call_ollama(prompt: str, model: str = "llama3.3:70b") -> Optional[str]:
    """Call Ollama API via subprocess. Returns response text or None on error."""
    import shutil

    # Find ollama binary (try PATH first, then common locations)
    ollama_bin = shutil.which("ollama")
    if not ollama_bin:
        for path in ["/usr/local/bin/ollama", "/opt/homebrew/bin/ollama"]:
            if Path(path).exists():
                ollama_bin = path
                break

    if not ollama_bin:
        print(f"  Ollama not found. Install: brew install ollama && ollama pull {model}")
        return None

    try:
        result = subprocess.run(
            [ollama_bin, "run", model],
            input=prompt,
            capture_output=True,
            text=True,
            timeout=300,  # 5 min timeout
        )
        if result.returncode != 0:
            print(f"  Ollama error: {result.stderr.strip()}")
            return None
        return result.stdout.strip()
    except subprocess.TimeoutExpired:
        print(f"  Ollama timeout (5 min)")
        return None
    except FileNotFoundError:
        print(f"  Ollama not found at {ollama_bin}")
        return None
    except Exception as e:
        print(f"  Ollama error: {e}")
        return None


def parse_enrichment_response(response: str) -> Optional[dict]:
    """Parse JSON response from LLM. Returns dict or None on parse error."""
    try:
        # Try to extract JSON from response (LLM might add extra text)
        start = response.find("{")
        end = response.rfind("}") + 1
        if start == -1 or end == 0:
            return None

        json_str = response[start:end]
        data = json.loads(json_str)

        # Validate required fields
        required = ["summary", "topics", "people", "sentiment"]
        if not all(k in data for k in required):
            return None

        return data
    except json.JSONDecodeError:
        return None


def enrich_asset(
    db: AtlasDB,
    asset_id: str,
    transcript_path: str,
    model: str = "llama3.3:70b",
    verbose: bool = False,
) -> dict:
    """
    Enrich a single asset with AI-generated metadata.

    Returns: {"status": "done"|"failed", "detail": str}
    """
    # Read transcript
    try:
        with open(transcript_path, "r") as f:
            transcript = f.read().strip()
    except FileNotFoundError:
        return {"status": "failed", "detail": "transcript file not found"}
    except Exception as e:
        return {"status": "failed", "detail": f"read error: {e}"}

    if not transcript:
        return {"status": "failed", "detail": "empty transcript"}

    # Truncate very long transcripts (Ollama context limits)
    MAX_CHARS = 15000  # ~3000 tokens
    if len(transcript) > MAX_CHARS:
        transcript = transcript[:MAX_CHARS] + "\n\n[...transcript truncated...]"

    # Generate prompt
    prompt = ENRICHMENT_PROMPT.format(transcript=transcript)

    if verbose:
        print(f"  Calling {model}...")

    # Call LLM
    response = call_ollama(prompt, model=model)
    if response is None:
        return {"status": "failed", "detail": "ollama call failed"}

    # Parse response
    data = parse_enrichment_response(response)
    if data is None:
        if verbose:
            print(f"  Raw response: {response[:200]}...")
        return {"status": "failed", "detail": "invalid JSON response"}

    # Update database
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    db.conn.execute("""
        UPDATE asset SET
            summary = ?,
            topics = ?,
            people = ?,
            sentiment = ?,
            enriched_at = ?
        WHERE id = ?
    """, (
        data["summary"],
        data["topics"],
        data["people"],
        data["sentiment"],
        now,
        asset_id,
    ))
    db.conn.commit()

    return {"status": "done", "detail": "enriched"}


def enrich_batch(
    config: Config,
    db: AtlasDB,
    limit: Optional[int] = None,
    model: str = "llama3.3:70b",
    verbose: bool = False,
    dry_run: bool = False,
) -> dict:
    """
    Enrich pending transcripts in batch.

    Returns dict with counts: done, failed, skipped.
    """
    # Get transcribed but not enriched assets
    query = """
        SELECT id, title, transcript_path, duration_sec
        FROM asset
        WHERE transcript_status = 'done'
          AND summary IS NULL
          AND transcript_path IS NOT NULL
        ORDER BY duration_sec ASC
    """
    if limit:
        query += f" LIMIT {int(limit)}"

    rows = db.conn.execute(query).fetchall()
    total = len(rows)

    if total == 0:
        print("No transcripts pending enrichment.")
        return {"done": 0, "failed": 0, "skipped": 0}

    # Summary
    total_duration = sum(r["duration_sec"] for r in rows)
    print(f"Enrichment batch: {total} transcripts, {total_duration/3600:.1f} hours")
    print(f"Model: {model}")
    print(f"Est. time: {total * 10 / 60:.1f}-{total * 30 / 60:.1f} minutes")
    print()

    if dry_run:
        print("DRY RUN — no enrichments will be performed.")
        for row in rows:
            print(f"  - {row['title'] or 'Untitled'} ({row['duration_sec']/60:.1f}min)")
        return {"done": 0, "failed": 0, "skipped": 0}

    counts = {"done": 0, "failed": 0, "skipped": 0}
    batch_start = time.time()

    for i, row in enumerate(rows, 1):
        asset_id = row["id"]
        title = row["title"] or "Untitled"
        transcript_path = row["transcript_path"]
        duration = row["duration_sec"]

        # Progress
        elapsed = time.time() - batch_start
        if counts["done"] > 0:
            avg_time = elapsed / (counts["done"] + counts["failed"])
            remaining = total - i + 1
            eta_seconds = remaining * avg_time
            eta_str = f"ETA: {eta_seconds/60:.1f}min"
        else:
            eta_str = "ETA: calculating..."

        print(f"[{i}/{total}] {title} ({duration/60:.1f}min) — {eta_str}")

        # Enrich
        start = time.time()
        result = enrich_asset(
            db, asset_id, transcript_path,
            model=model, verbose=verbose
        )
        elapsed_item = time.time() - start

        if result["status"] == "done":
            print(f"  OK ({elapsed_item:.1f}s)")
            counts["done"] += 1
        else:
            print(f"  FAILED: {result['detail']}")
            counts["failed"] += 1

    # Summary
    total_elapsed = time.time() - batch_start
    print()
    print(f"Batch complete: {counts['done']} done, {counts['failed']} failed, {counts['skipped']} skipped")
    print(f"Time: {total_elapsed/60:.1f} minutes")
    if counts["done"] > 0:
        print(f"Avg: {total_elapsed/counts['done']:.1f}s per enrichment")

    return counts
