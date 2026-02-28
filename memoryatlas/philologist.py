"""Philological enhancement: source-language restoration + literary translation.

Two-stage pipeline for non-English transcripts:
  Stage 1 (Restore): Clean Whisper artifacts, restore grammar, preserve voice
  Stage 2 (Translate): Literary-quality English translation, register-matched

Designed for Russian voice memos but language-agnostic in principle.
Uses local Ollama for privacy (voice memos are intimate material).
"""
import json
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional

from .config import Config
from .db import AtlasDB


# ---------------------------------------------------------------------------
# Stage 1: Source Restoration
# ---------------------------------------------------------------------------

RESTORE_PROMPT = """You are a philological editor restoring a voice memo transcript.
The transcript was produced by Whisper AI and contains artifacts that must be cleaned.

YOUR TASK: Restore this transcript to the best version of what the speaker actually said and meant.

RULES:
1. **Remove hallucination spam**: Whisper sometimes hallucinates repeated words/phrases
   hundreds of times (e.g. "stron stron stron...", "Субтитры сделал DimaTorzok...",
   "Продолжение следует...", "велен велен...", "hunter hunter...").
   DELETE these entirely — they are machine artifacts, not speech.

2. **Reduce fillers**: Keep SOME natural fillers ("Угу", "Ну", "Эм", "Ага") to preserve
   conversational rhythm, but consolidate excessive repetition. If "Угу" appears 40 times
   consecutively, keep 1-2 instances. The voice should breathe, not stutter.

3. **Restore grammar & punctuation**: Add proper sentence boundaries, paragraph breaks,
   and punctuation. The speaker is educated — restore their register, not a simplified version.

4. **Preserve code-switching**: If the speaker shifts between Russian and English (or other
   languages), keep BOTH languages exactly as spoken. Code-switching is meaningful.

5. **Preserve the voice**: This is someone's real speech. Keep their word choices, their
   hesitations where meaningful, their humor, their emotional register. You are restoring,
   not rewriting.

6. **Add speaker attribution**: If you can distinguish multiple speakers from context
   (e.g. a conversation), mark them as [Speaker A], [Speaker B], etc. If one speaker
   is clearly interviewing/asking and another answering, use [Q] and [A].

7. **Flag uncertainty**: If a passage is genuinely unclear (not just a Whisper artifact
   but actual unintelligible speech), mark it as [unclear].

8. **Paragraph structure**: Break the text into logical paragraphs by topic shifts,
   speaker changes, or natural pauses in thought.

TRANSCRIPT LANGUAGE: {language}

RAW TRANSCRIPT:
{transcript}

Respond with ONLY the restored text. No commentary, no explanations, no JSON wrapping.
Just the clean, restored transcript."""


# ---------------------------------------------------------------------------
# Stage 2: Literary Translation
# ---------------------------------------------------------------------------

TRANSLATE_PROMPT = """You are a literary translator working with a restored voice memo transcript.
The speaker's context: this is from a personal voice memo collection belonging to a
Russian-American family. The recordings span intimate family conversations, intellectual
discussions, immigration stories, and daily life.

YOUR TASK: Translate this transcript into English with literary quality.
Not literal translation — faithful and elevated. The best meaning that stays true.

PRINCIPLES:
1. **Register-match**: If the speaker is an educated literary scholar discussing Chekhov,
   the English should reflect that erudition. If they're talking about plumbing, keep it
   vernacular. If a father is telling his son about fleeing the Soviet Union, the English
   should carry that weight and tenderness.

2. **Preserve rhetorical force**: Political speech should thunder. Intimate speech should
   be tender. Humor should land. Don't flatten everything into neutral prose.

3. **Cultural resonance**: When a concept has no direct English equivalent, translate the
   meaning AND briefly note the cultural context in [brackets]. For example:
   "артель" → "artel [a Soviet-era workers' cooperative, neither fully private nor state-owned]"

4. **Untranslatable moments**: Some words carry worlds. When a Russian word is the right
   word even in English, keep it with a gloss: "тоска [that uniquely Russian longing]".
   Use this sparingly — only for words that genuinely resist translation.

5. **Code-switching preserved**: If the speaker switches to English in the original,
   mark it: *[in English in original:]* "your soldiers will be torn apart."

6. **Speaker attribution**: Preserve any [Speaker A]/[Speaker B] or [Q]/[A] markers
   from the restored text.

7. **Paragraph structure**: Match the paragraph breaks of the source.

SOURCE LANGUAGE: {language}

RESTORED TRANSCRIPT:
{transcript}

Respond with ONLY the English translation. No commentary, no explanations.
Just the literary translation."""


# ---------------------------------------------------------------------------
# Artifact detection (pre-processing before LLM)
# ---------------------------------------------------------------------------

# Patterns that indicate Whisper hallucination spam
SPAM_PATTERNS = [
    r'(?:Субтитры сделал \w+\s*){3,}',
    r'(?:Добавил субтитры \w+\s*){3,}',
    r'(?:Продолжение следует\.{3}\s*){3,}',
    r'(?:stron\s+){5,}',
    r'(?:велен\s+){5,}',
    r'(?:hunter\s+){5,}',
    r'(?:в числе\s+){5,}',
    r'(?:в этом числе\s+){5,}',
    r'(?:в кироп fiscal\s+){3,}',
    r'(?:Угу\.\s*){10,}',  # 10+ consecutive "Угу" is hallucination
    r'(?:ж\s+){10,}',
    r'(?:(?:стелен)?еленеленелен\w*\s*){3,}',
]


def call_ollama_api(prompt: str, model: str = "qwen2.5:32b") -> Optional[str]:
    """Call Ollama via HTTP API. More reliable than CLI (no spinner pollution)."""
    import urllib.request
    import urllib.error

    url = "http://localhost:11434/api/generate"
    payload = json.dumps({
        "model": model,
        "prompt": prompt,
        "stream": False,
        "options": {
            "num_ctx": 8192,
            "temperature": 0.3,  # Low temperature for faithful restoration
        }
    }).encode("utf-8")

    req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=600) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            return data.get("response", "").strip()
    except urllib.error.URLError as e:
        print(f"  Ollama API error: {e}")
        return None
    except TimeoutError:
        print(f"  Ollama API timeout (10 min)")
        return None
    except Exception as e:
        print(f"  Ollama error: {e}")
        return None


def pre_clean(text: str) -> str:
    """Remove obvious Whisper hallucination spam before sending to LLM.

    This saves tokens and prevents the LLM from being confused by massive
    blocks of repeated nonsense. We do the mechanical cleaning; the LLM
    does the philological restoration.
    """
    cleaned = text
    for pattern in SPAM_PATTERNS:
        cleaned = re.sub(pattern, ' [hallucination removed] ', cleaned, flags=re.IGNORECASE)

    # Collapse excessive whitespace
    cleaned = re.sub(r'\n{4,}', '\n\n\n', cleaned)
    cleaned = re.sub(r' {3,}', ' ', cleaned)

    return cleaned.strip()


# ---------------------------------------------------------------------------
# Core functions
# ---------------------------------------------------------------------------

def restore_transcript(
    transcript: str,
    language: str = "Russian",
    model: str = "qwen2.5:32b",
    verbose: bool = False,
) -> Optional[str]:
    """Stage 1: Restore a raw transcript to its best form in the source language."""
    # Pre-clean mechanical spam
    cleaned = pre_clean(transcript)

    # Truncate for LLM context
    MAX_CHARS = 12000  # Leave room for prompt + response
    if len(cleaned) > MAX_CHARS:
        cleaned = cleaned[:MAX_CHARS] + "\n\n[...transcript truncated...]"

    prompt = RESTORE_PROMPT.format(transcript=cleaned, language=language)

    if verbose:
        print(f"  Stage 1 (Restore): sending {len(cleaned)} chars to {model}...")

    response = call_ollama_api(prompt, model=model)
    if response is None:
        return None

    # Strip any accidental JSON wrapping or code blocks
    result = response.strip()
    if result.startswith("```"):
        result = re.sub(r'^```\w*\n?', '', result)
        result = re.sub(r'\n?```$', '', result)

    return result


def translate_transcript(
    restored_text: str,
    language: str = "Russian",
    model: str = "qwen2.5:32b",
    verbose: bool = False,
) -> Optional[str]:
    """Stage 2: Literary translation of restored transcript to English."""
    MAX_CHARS = 12000
    text = restored_text
    if len(text) > MAX_CHARS:
        text = text[:MAX_CHARS] + "\n\n[...text truncated...]"

    prompt = TRANSLATE_PROMPT.format(transcript=text, language=language)

    if verbose:
        print(f"  Stage 2 (Translate): sending {len(text)} chars to {model}...")

    response = call_ollama_api(prompt, model=model)
    if response is None:
        return None

    result = response.strip()
    if result.startswith("```"):
        result = re.sub(r'^```\w*\n?', '', result)
        result = re.sub(r'\n?```$', '', result)

    return result


def polish_asset(
    db: AtlasDB,
    asset_id: str,
    transcript_path: str,
    language: str = "Russian",
    model: str = "qwen2.5:32b",
    data_dir: Optional[Path] = None,
    verbose: bool = False,
) -> dict:
    """Run full philological pipeline on a single asset.

    Returns: {"status": "done"|"failed"|"partial", "detail": str}
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

    # Stage 1: Restore
    restored = restore_transcript(transcript, language=language, model=model, verbose=verbose)
    if restored is None:
        return {"status": "failed", "detail": "restoration failed (ollama)"}

    # Save restored text
    if data_dir:
        polished_dir = data_dir / "polished"
        polished_dir.mkdir(parents=True, exist_ok=True)
        restored_path = polished_dir / f"{asset_id}_restored.txt"
        restored_path.write_text(restored, encoding="utf-8")

    # Stage 2: Translate
    translated = translate_transcript(restored, language=language, model=model, verbose=verbose)
    if translated is None:
        # Partial success — we have the restoration but not the translation
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        db.conn.execute("""
            UPDATE asset SET
                restored_text = ?,
                polished_at = ?
            WHERE id = ?
        """, (restored, now, asset_id))
        db.conn.commit()
        return {"status": "partial", "detail": "restored but translation failed"}

    # Save translation
    if data_dir:
        translated_path = polished_dir / f"{asset_id}_translated.txt"
        translated_path.write_text(translated, encoding="utf-8")

    # Update database
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    db.conn.execute("""
        UPDATE asset SET
            restored_text = ?,
            translated_text = ?,
            polished_at = ?
        WHERE id = ?
    """, (restored, translated, now, asset_id))
    db.conn.commit()

    return {"status": "done", "detail": "restored + translated"}


# ---------------------------------------------------------------------------
# Batch processing
# ---------------------------------------------------------------------------

LANG_MAP = {
    "ru": "Russian",
    "uk": "Ukrainian",
    "ja": "Japanese",
    "ko": "Korean",
    "tr": "Turkish",
    "pt": "Portuguese",
    "is": "Icelandic",
}


def polish_batch(
    config: Config,
    db: AtlasDB,
    limit: Optional[int] = None,
    model: str = "qwen2.5:32b",
    language: Optional[str] = None,
    verbose: bool = False,
    dry_run: bool = False,
) -> dict:
    """Run philological pipeline on non-English transcripts.

    By default, processes all non-English transcripts that haven't been polished yet.
    Use --language to restrict to a specific language code (e.g., 'ru').
    """
    # Build query for unpolished non-English transcripts
    lang_filter = ""
    params = []
    if language:
        lang_filter = "AND transcript_lang = ?"
        params.append(language)
    else:
        lang_filter = "AND transcript_lang != 'en' AND transcript_lang IS NOT NULL"

    query = f"""
        SELECT id, title, transcript_path, transcript_lang, duration_sec
        FROM asset
        WHERE transcript_status = 'done'
          AND polished_at IS NULL
          AND transcript_path IS NOT NULL
          {lang_filter}
        ORDER BY duration_sec ASC
    """
    if limit:
        query += f" LIMIT {int(limit)}"

    rows = db.conn.execute(query, params).fetchall()
    total = len(rows)

    if total == 0:
        print("No transcripts pending philological enhancement.")
        return {"done": 0, "failed": 0, "partial": 0}

    total_duration = sum(r["duration_sec"] for r in rows)
    print(f"Philological batch: {total} transcripts, {total_duration/3600:.1f} hours")
    print(f"Model: {model}")
    print(f"Pipeline: Restore (source language) → Translate (English)")
    print(f"Est. time: {total * 30 / 60:.1f}-{total * 90 / 60:.1f} minutes")
    print()

    if dry_run:
        print("DRY RUN — no processing will be performed.")
        for row in rows:
            lang = row["transcript_lang"]
            lang_name = LANG_MAP.get(lang, lang)
            print(f"  - [{lang}] {row['title'] or 'Untitled'} ({row['duration_sec']/60:.1f}min) → {lang_name}")
        return {"done": 0, "failed": 0, "partial": 0}

    counts = {"done": 0, "failed": 0, "partial": 0}
    batch_start = time.time()

    for i, row in enumerate(rows, 1):
        asset_id = row["id"]
        title = row["title"] or "Untitled"
        lang_code = row["transcript_lang"]
        lang_name = LANG_MAP.get(lang_code, lang_code or "Unknown")
        duration = row["duration_sec"]

        # Progress
        elapsed = time.time() - batch_start
        processed = counts["done"] + counts["failed"] + counts["partial"]
        if processed > 0:
            avg_time = elapsed / processed
            remaining = total - i + 1
            eta_str = f"ETA: {remaining * avg_time / 60:.1f}min"
        else:
            eta_str = "ETA: calculating..."

        print(f"[{i}/{total}] [{lang_code}] {title} ({duration/60:.1f}min) — {eta_str}")

        start = time.time()
        result = polish_asset(
            db, asset_id, row["transcript_path"],
            language=lang_name,
            model=model,
            data_dir=config.data_dir,
            verbose=verbose,
        )
        elapsed_item = time.time() - start

        status = result["status"]
        print(f"  {status.upper()} ({elapsed_item:.1f}s): {result['detail']}")
        counts[status] = counts.get(status, 0) + 1

    total_elapsed = time.time() - batch_start
    print()
    print(f"Batch complete: {counts['done']} done, {counts['partial']} partial, {counts['failed']} failed")
    print(f"Time: {total_elapsed/60:.1f} minutes")

    return counts
