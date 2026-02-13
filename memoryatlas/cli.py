"""CLI interface using Typer."""
import typer
from pathlib import Path
from typing import Optional

from .config import Config
from .db import AtlasDB
from .scanner import scan as do_scan
from .publisher import publish as do_publish, publish_index, publish_about
from .transcriber import transcribe_batch
from .util import format_count_line
from .constants import VERSION

app = typer.Typer(
    name="atlas",
    help=f"MemoryAtlas v{VERSION}: voice memos to Obsidian notes.",
    no_args_is_help=True,
)


def _load(config_path: Optional[Path] = None) -> tuple:
    config = Config.load(config_path)
    db = AtlasDB(config.db_path)
    return config, db


@app.command()
def init(
    config_path: Optional[Path] = typer.Option(None, "--config", "-c"),
):
    """Initialize the MemoryAtlas database and vault folder."""
    config, db = _load(config_path)

    with db:
        db.init_schema()
        print(f"  Database: {config.db_path}")

    (config.atlas_vault_dir / "voice").mkdir(parents=True, exist_ok=True)
    print(f"  Vault:    {config.atlas_vault_dir}")

    publish_about(config)
    print(f"  Docs:     {config.atlas_vault_dir / '_About.md'}")

    print("\nInitialized. Run 'atlas scan' to import voice memos.")


@app.command("scan")
def scan_cmd(
    config_path: Optional[Path] = typer.Option(None, "--config", "-c"),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
):
    """Scan Apple Voice Memos and import metadata."""
    config, db = _load(config_path)

    with db:
        db.init_schema()
        print("Scanning Apple Voice Memos...")
        counts = do_scan(config, db, verbose=verbose)
        print(f"Done: {format_count_line(counts)}")


@app.command("publish")
def publish_cmd(
    config_path: Optional[Path] = typer.Option(None, "--config", "-c"),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
    force: bool = typer.Option(False, "--force", "-f", help="Regenerate all notes"),
    index_only: bool = typer.Option(False, "--index-only", help="Only update the index"),
):
    """Publish Obsidian notes for scanned assets."""
    config, db = _load(config_path)

    with db:
        if not index_only:
            print("Publishing notes...")
            counts = do_publish(config, db, verbose=verbose, force=force)
            print(f"Done: {format_count_line(counts)}")

        print("Updating index...")
        publish_index(config, db)
        print(f"  {config.atlas_vault_dir / '_Index.md'}")


@app.command()
def status(
    config_path: Optional[Path] = typer.Option(None, "--config", "-c"),
):
    """Show MemoryAtlas statistics."""
    config, db = _load(config_path)

    with db:
        stats = db.get_stats()
        print(f"MemoryAtlas v{VERSION}")
        print(f"{'=' * 35}")
        print(f"  Total assets:  {stats['total']}")
        print(f"  Total hours:   {stats['total_hours']:.1f}")
        print(f"  Published:     {stats['published']}")
        print(f"  Transcribed:   {stats['transcribed']}")
        print(f"  Enriched:      {stats['enriched']}")
        print(f"")
        print(f"  Database: {config.db_path}")
        print(f"  Vault:    {config.atlas_vault_dir}")


@app.command()
def doctor(
    config_path: Optional[Path] = typer.Option(None, "--config", "-c"),
):
    """Check system health."""
    config, _ = _load(config_path)
    issues = []
    ok = []

    if config.apple_db_path.exists():
        ok.append(f"Apple Voice Memos DB: {config.apple_db_path}")
    else:
        issues.append(f"Apple Voice Memos DB not found: {config.apple_db_path}")

    if config.db_path.exists():
        ok.append(f"Atlas DB: {config.db_path}")
    else:
        issues.append(f"Atlas DB not found (run 'atlas init'): {config.db_path}")

    if config.atlas_vault_dir.exists():
        ok.append(f"Vault dir: {config.atlas_vault_dir}")
    else:
        issues.append(f"Vault dir not found (run 'atlas init'): {config.atlas_vault_dir}")

    whisper_bin = config.whisper_env / "bin" / "whisper"
    if whisper_bin.exists():
        ok.append(f"Whisper: {whisper_bin}")
    else:
        issues.append(f"Whisper not found: {whisper_bin}")

    import shutil
    ffmpeg = shutil.which("ffmpeg")
    if ffmpeg:
        ok.append(f"ffmpeg: {ffmpeg}")
    else:
        # Check common locations
        for p in ["/usr/local/bin/ffmpeg", "/opt/homebrew/bin/ffmpeg"]:
            if Path(p).exists():
                ok.append(f"ffmpeg: {p} (not in PATH)")
                break
        else:
            issues.append("ffmpeg not found")

    for item in ok:
        print(f"  OK  {item}")
    for item in issues:
        print(f"  !!  {item}")

    if not issues:
        print(f"\nAll {len(ok)} checks passed.")
    else:
        print(f"\n{len(issues)} issue(s) found.")
        raise typer.Exit(1)


@app.command("transcribe")
def transcribe_cmd(
    config_path: Optional[Path] = typer.Option(None, "--config", "-c"),
    limit: Optional[int] = typer.Option(None, "--limit", "-n", help="Max files to transcribe"),
    model: str = typer.Option(
        "mlx-community/whisper-turbo",
        "--model", "-m",
        help="HuggingFace model ID (mlx-community/whisper-turbo, mlx-community/whisper-large-v3-mlx)",
    ),
    language: Optional[str] = typer.Option(None, "--language", "-l", help="Force language (en, ru, etc.)"),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be transcribed"),
):
    """Transcribe pending voice memos using mlx-whisper."""
    config, db = _load(config_path)

    with db:
        db.init_schema()
        counts = transcribe_batch(
            config, db,
            limit=limit,
            model=model,
            language=language,
            verbose=verbose,
            dry_run=dry_run,
        )

        if counts["done"] > 0:
            print("\nRepublishing transcribed notes...")
            pub_counts = do_publish(config, db, force=True)
            print(f"Notes updated: {pub_counts['updated']}")
            publish_index(config, db)


@app.command()
def info(
    asset_id: str = typer.Argument(..., help="Asset UUID (full or prefix)"),
    config_path: Optional[Path] = typer.Option(None, "--config", "-c"),
):
    """Show details for a single asset."""
    config, db = _load(config_path)

    with db:
        row = db.get_asset(asset_id)
        if row is None:
            row = db.conn.execute(
                "SELECT * FROM asset WHERE id LIKE ?", (f"{asset_id}%",)
            ).fetchone()

        if row is None:
            print(f"Asset not found: {asset_id}")
            raise typer.Exit(1)

        from .util import row_to_asset
        asset = row_to_asset(row)

        print(f"  ID:         {asset.id}")
        print(f"  Title:      {asset.title}")
        print(f"  Recorded:   {asset.recorded_at}")
        print(f"  Duration:   {asset.duration_display}")
        print(f"  Format:     .{asset.file_format}")
        print(f"  Source:     {asset.source_path}")
        print(f"  Transcript: {asset.transcript_status}")
        print(f"  Note:       {asset.note_path or '(not published)'}")


def main():
    app()
