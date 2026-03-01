"""Philological enhancement: source-language restoration + literary translation.

Two-stage pipeline for non-English transcripts:
  Stage 1 (Restore): Clean Whisper artifacts, restore grammar, preserve voice
  Stage 2 (Translate): Literary-quality English translation, register-matched

Designed for Russian voice memos but language-agnostic in principle.
Supports two backends:
  - Ollama (local, private, default for bulk processing)
  - Claude API (cloud, literary-tier quality for crown jewels)
"""
import json
import re
import time
import signal
import sys
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


# ---------------------------------------------------------------------------
# Ollama health & resilience
# ---------------------------------------------------------------------------

def check_ollama_health(model: str = "qwen2.5:32b") -> dict:
    """Check Ollama server health and model availability.

    Returns: {"healthy": bool, "models_loaded": list, "model_available": bool, "detail": str}
    """
    import urllib.request
    import urllib.error

    result = {"healthy": False, "models_loaded": [], "model_available": False, "detail": ""}

    # Check if Ollama is responding
    try:
        req = urllib.request.Request("http://localhost:11434/api/tags")
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            result["healthy"] = True
            models = [m["name"] for m in data.get("models", [])]
            result["models_loaded"] = models
            result["model_available"] = any(model in m for m in models)
            result["detail"] = f"Ollama up, {len(models)} models"
    except urllib.error.URLError:
        result["detail"] = "Ollama not responding on localhost:11434"
    except Exception as e:
        result["detail"] = f"Health check error: {e}"

    return result


def preload_model(model: str = "qwen2.5:32b") -> bool:
    """Warm up a model by sending a trivial prompt. Returns True if successful."""
    import urllib.request
    import urllib.error

    print(f"  Preloading model {model}...")
    payload = json.dumps({
        "model": model,
        "prompt": "Hello",
        "stream": False,
        "options": {"num_predict": 1}
    }).encode("utf-8")

    req = urllib.request.Request(
        "http://localhost:11434/api/generate",
        data=payload,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            json.loads(resp.read().decode("utf-8"))
            print(f"  Model {model} loaded and ready.")
            return True
    except Exception as e:
        print(f"  Model preload failed: {e}")
        return False


def call_ollama_api(
    prompt: str,
    model: str = "qwen2.5:32b",
    max_retries: int = 3,
    base_delay: float = 5.0,
) -> Optional[str]:
    """Call Ollama via HTTP API with exponential backoff retry.

    Retries on transient failures (connection refused, timeout).
    Does NOT retry on permanent failures (model not found, bad request).
    """
    import urllib.request
    import urllib.error

    url = "http://localhost:11434/api/generate"
    payload = json.dumps({
        "model": model,
        "prompt": prompt,
        "stream": False,
        "options": {
            "num_ctx": 8192,
            "temperature": 0.3,
        }
    }).encode("utf-8")

    for attempt in range(max_retries):
        req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=600) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                return data.get("response", "").strip()

        except urllib.error.URLError as e:
            reason = str(e.reason) if hasattr(e, 'reason') else str(e)
            is_transient = any(k in reason.lower() for k in [
                "connection refused", "connection reset", "temporary",
                "timed out", "service unavailable",
            ])
            if is_transient and attempt < max_retries - 1:
                delay = base_delay * (3 ** attempt)  # 5s, 15s, 45s
                print(f"  Ollama connection error (attempt {attempt + 1}/{max_retries}), "
                      f"retrying in {delay:.0f}s: {reason}")
                time.sleep(delay)
                continue
            print(f"  Ollama API error (final): {reason}")
            return None

        except TimeoutError:
            if attempt < max_retries - 1:
                delay = base_delay * (3 ** attempt)
                print(f"  Ollama timeout (attempt {attempt + 1}/{max_retries}), "
                      f"retrying in {delay:.0f}s")
                time.sleep(delay)
                continue
            print(f"  Ollama API timeout (final, 10 min)")
            return None

        except Exception as e:
            print(f"  Ollama error: {e}")
            return None

    return None


# ---------------------------------------------------------------------------
# Claude API backend (literary-tier)
# ---------------------------------------------------------------------------

def call_claude_api(
    prompt: str,
    model: str = "claude-sonnet-4-5-20250929",
    max_tokens: int = 8192,
) -> Optional[str]:
    """Call Claude API for literary-tier translations.

    Requires ANTHROPIC_API_KEY environment variable.
    Falls back gracefully if not available.
    """
    import os
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("  Claude API: ANTHROPIC_API_KEY not set, skipping")
        return None

    import urllib.request
    import urllib.error

    url = "https://api.anthropic.com/v1/messages"
    payload = json.dumps({
        "model": model,
        "max_tokens": max_tokens,
        "messages": [{"role": "user", "content": prompt}],
    }).encode("utf-8")

    req = urllib.request.Request(url, data=payload, headers={
        "Content-Type": "application/json",
        "x-api-key": api_key,
        "anthropic-version": "2023-06-01",
    })

    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            content = data.get("content", [])
            if content and content[0].get("type") == "text":
                return content[0]["text"].strip()
            return None
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")[:200]
        print(f"  Claude API HTTP {e.code}: {body}")
        return None
    except Exception as e:
        print(f"  Claude API error: {e}")
        return None


# ---------------------------------------------------------------------------
# Unified LLM caller
# ---------------------------------------------------------------------------

def call_llm(
    prompt: str,
    backend: str = "ollama",
    model: Optional[str] = None,
    verbose: bool = False,
) -> Optional[str]:
    """Route to the appropriate LLM backend.

    backend: "ollama" (default), "claude", "auto"
    auto: try Claude first, fall back to Ollama
    """
    if backend == "claude":
        claude_model = model or "claude-sonnet-4-5-20250929"
        if verbose:
            print(f"  Using Claude API ({claude_model})...")
        return call_claude_api(prompt, model=claude_model)

    elif backend == "auto":
        # Try Claude first for quality, fall back to Ollama
        claude_model = model if model and "claude" in model else "claude-sonnet-4-5-20250929"
        if verbose:
            print(f"  Trying Claude API ({claude_model})...")
        result = call_claude_api(prompt, model=claude_model)
        if result:
            return result
        if verbose:
            print(f"  Claude unavailable, falling back to Ollama...")
        ollama_model = model if model and "claude" not in model else "qwen2.5:32b"
        return call_ollama_api(prompt, model=ollama_model)

    else:  # ollama (default)
        ollama_model = model or "qwen2.5:32b"
        return call_ollama_api(prompt, model=ollama_model)


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
    backend: str = "ollama",
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
        print(f"  Stage 1 (Restore): sending {len(cleaned)} chars to {backend}/{model}...")

    response = call_llm(prompt, backend=backend, model=model, verbose=verbose)
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
    backend: str = "ollama",
    verbose: bool = False,
) -> Optional[str]:
    """Stage 2: Literary translation of restored transcript to English."""
    MAX_CHARS = 12000
    text = restored_text
    if len(text) > MAX_CHARS:
        text = text[:MAX_CHARS] + "\n\n[...text truncated...]"

    prompt = TRANSLATE_PROMPT.format(transcript=text, language=language)

    if verbose:
        print(f"  Stage 2 (Translate): sending {len(text)} chars to {backend}/{model}...")

    response = call_llm(prompt, backend=backend, model=model, verbose=verbose)
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
    backend: str = "ollama",
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
    restored = restore_transcript(
        transcript, language=language, model=model,
        backend=backend, verbose=verbose,
    )
    if restored is None:
        return {"status": "failed", "detail": "restoration failed (llm)"}

    # Save restored text
    if data_dir:
        polished_dir = data_dir / "polished"
        polished_dir.mkdir(parents=True, exist_ok=True)
        restored_path = polished_dir / f"{asset_id}_restored.txt"
        restored_path.write_text(restored, encoding="utf-8")

    # Stage 2: Translate
    translated = translate_transcript(
        restored, language=language, model=model,
        backend=backend, verbose=verbose,
    )
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
# SRT subtitle processing
# ---------------------------------------------------------------------------

def parse_srt(text: str) -> list:
    """Parse SRT subtitle format into a list of entries.

    Each entry: {"index": int, "start": str, "end": str, "text": str}
    """
    entries = []
    blocks = re.split(r'\n\s*\n', text.strip())

    for block in blocks:
        lines = block.strip().split('\n')
        if len(lines) < 3:
            continue

        try:
            index = int(lines[0].strip())
        except ValueError:
            continue

        time_match = re.match(
            r'(\d{2}:\d{2}:\d{2},\d{3})\s*-->\s*(\d{2}:\d{2}:\d{2},\d{3})',
            lines[1].strip()
        )
        if not time_match:
            continue

        text_content = '\n'.join(lines[2:]).strip()
        entries.append({
            "index": index,
            "start": time_match.group(1),
            "end": time_match.group(2),
            "text": text_content,
        })

    return entries


def write_srt(entries: list) -> str:
    """Write SRT entries back to string format."""
    blocks = []
    for i, entry in enumerate(entries, 1):
        blocks.append(f"{i}\n{entry['start']} --> {entry['end']}\n{entry['text']}")
    return '\n\n'.join(blocks) + '\n'


def polish_srt(
    srt_path: str,
    output_path: Optional[str] = None,
    language: str = "Russian",
    model: str = "qwen2.5:32b",
    backend: str = "ollama",
    mode: str = "translate",
    verbose: bool = False,
) -> dict:
    """Process an SRT file through the philological pipeline.

    mode:
      "translate" — translate subtitles to English (preserving timing)
      "restore" — restore source language (clean Whisper artifacts)
      "both" — restore then translate (outputs two files)

    Returns: {"status": "done"|"failed", "detail": str, "output_files": list}
    """
    srt_file = Path(srt_path)
    if not srt_file.exists():
        return {"status": "failed", "detail": f"SRT file not found: {srt_path}"}

    text = srt_file.read_text(encoding="utf-8")
    entries = parse_srt(text)

    if not entries:
        return {"status": "failed", "detail": "No valid SRT entries found"}

    print(f"SRT: {len(entries)} subtitle entries, language: {language}")

    # Concatenate all subtitle text for batch processing
    # (processing entry-by-entry is too slow and loses context)
    full_text = '\n'.join(f"[{e['index']}] {e['text']}" for e in entries)

    output_files = []
    stem = srt_file.stem
    parent = Path(output_path) if output_path else srt_file.parent

    if mode in ("restore", "both"):
        print("  Stage 1: Restoring source language...")
        restore_prompt = RESTORE_PROMPT.format(transcript=full_text, language=language)
        restore_prompt += "\n\nIMPORTANT: Preserve the [number] markers at the start of each line."
        restored = call_llm(restore_prompt, backend=backend, model=model, verbose=verbose)

        if restored:
            restored_entries = _reattach_timing(restored, entries)
            restored_srt = write_srt(restored_entries)
            out_path = parent / f"{stem}-restored.srt"
            out_path.write_text(restored_srt, encoding="utf-8")
            output_files.append(str(out_path))
            print(f"  Restored: {out_path}")
        else:
            return {"status": "failed", "detail": "SRT restoration failed"}

    if mode in ("translate", "both"):
        source_text = full_text
        if mode == "both" and restored:
            source_text = restored

        print("  Stage 2: Literary translation...")
        translate_prompt = TRANSLATE_PROMPT.format(transcript=source_text, language=language)
        translate_prompt += "\n\nIMPORTANT: Preserve the [number] markers at the start of each line."
        translated = call_llm(translate_prompt, backend=backend, model=model, verbose=verbose)

        if translated:
            translated_entries = _reattach_timing(translated, entries)
            translated_srt = write_srt(translated_entries)
            out_path = parent / f"{stem}-en.srt"
            out_path.write_text(translated_srt, encoding="utf-8")
            output_files.append(str(out_path))
            print(f"  Translated: {out_path}")
        else:
            return {"status": "failed", "detail": "SRT translation failed"}

    return {
        "status": "done",
        "detail": f"Processed {len(entries)} entries",
        "output_files": output_files,
    }


def _reattach_timing(llm_output: str, original_entries: list) -> list:
    """Reattach timing from original SRT entries to LLM-processed text.

    The LLM processes text with [index] markers. We match those back to
    the original timing codes. Unmatched entries keep original text.
    """
    # Parse LLM output: look for [number] markers
    processed = {}
    current_idx = None
    current_lines = []

    for line in llm_output.split('\n'):
        marker = re.match(r'\[(\d+)\]\s*(.*)', line.strip())
        if marker:
            if current_idx is not None:
                processed[current_idx] = '\n'.join(current_lines).strip()
            current_idx = int(marker.group(1))
            current_lines = [marker.group(2)] if marker.group(2) else []
        elif current_idx is not None:
            current_lines.append(line.strip())

    if current_idx is not None:
        processed[current_idx] = '\n'.join(current_lines).strip()

    # Rebuild entries with original timing
    result = []
    for entry in original_entries:
        new_text = processed.get(entry["index"], entry["text"])
        result.append({
            "index": entry["index"],
            "start": entry["start"],
            "end": entry["end"],
            "text": new_text if new_text else entry["text"],
        })

    return result


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
    "ar": "Arabic",
    "zh": "Chinese",
    "de": "German",
    "fr": "French",
    "es": "Spanish",
    "it": "Italian",
    "he": "Hebrew",
}

# Batch interrupt flag for graceful shutdown
_batch_interrupted = False


def _handle_interrupt(signum, frame):
    """Handle SIGINT gracefully during batch processing."""
    global _batch_interrupted
    _batch_interrupted = True
    print("\n  Interrupt received. Finishing current item then stopping...")


def polish_batch(
    config: Config,
    db: AtlasDB,
    limit: Optional[int] = None,
    model: str = "qwen2.5:32b",
    backend: str = "ollama",
    language: Optional[str] = None,
    verbose: bool = False,
    dry_run: bool = False,
) -> dict:
    """Run philological pipeline on non-English transcripts.

    By default, processes all non-English transcripts that haven't been polished yet.
    Use --language to restrict to a specific language code (e.g., 'ru').
    """
    global _batch_interrupted
    _batch_interrupted = False

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
    print(f"Backend: {backend}, Model: {model}")
    print(f"Pipeline: Restore (source language) -> Translate (English)")
    print(f"Est. time: {total * 30 / 60:.1f}-{total * 90 / 60:.1f} minutes")
    print()

    if dry_run:
        print("DRY RUN — no processing will be performed.")
        for row in rows:
            lang = row["transcript_lang"]
            lang_name = LANG_MAP.get(lang, lang)
            print(f"  - [{lang}] {row['title'] or 'Untitled'} ({row['duration_sec']/60:.1f}min) -> {lang_name}")
        return {"done": 0, "failed": 0, "partial": 0}

    # Pre-flight checks for Ollama backend
    if backend in ("ollama", "auto"):
        health = check_ollama_health(model)
        if not health["healthy"]:
            print(f"ERROR: {health['detail']}")
            print("Start Ollama: ollama serve")
            return {"done": 0, "failed": 0, "partial": 0}

        if not health["model_available"]:
            print(f"Model {model} not found. Available: {', '.join(health['models_loaded'])}")
            print(f"Pull it: ollama pull {model}")
            return {"done": 0, "failed": 0, "partial": 0}

        # Warm up the model
        preload_model(model)

    # Install interrupt handler for graceful shutdown
    old_handler = signal.getsignal(signal.SIGINT)
    signal.signal(signal.SIGINT, _handle_interrupt)

    counts = {"done": 0, "failed": 0, "partial": 0}
    consecutive_failures = 0
    MAX_CONSECUTIVE_FAILURES = 5
    batch_start = time.time()

    try:
        for i, row in enumerate(rows, 1):
            if _batch_interrupted:
                print(f"\nBatch interrupted at item {i}/{total}. Progress saved.")
                break

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
                backend=backend,
                data_dir=config.data_dir,
                verbose=verbose,
            )
            elapsed_item = time.time() - start

            status = result["status"]
            print(f"  {status.upper()} ({elapsed_item:.1f}s): {result['detail']}")
            counts[status] = counts.get(status, 0) + 1

            # Track consecutive failures for circuit breaker
            if status == "failed":
                consecutive_failures += 1
                if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                    print(f"\n  {MAX_CONSECUTIVE_FAILURES} consecutive failures — "
                          f"stopping batch (Ollama may have crashed).")
                    print(f"  Check: curl http://localhost:11434/api/tags")
                    break
            else:
                consecutive_failures = 0

    finally:
        # Restore original signal handler
        signal.signal(signal.SIGINT, old_handler)

    total_elapsed = time.time() - batch_start
    print()
    print(f"Batch complete: {counts['done']} done, {counts['partial']} partial, {counts['failed']} failed")
    print(f"Time: {total_elapsed/60:.1f} minutes")

    return counts
