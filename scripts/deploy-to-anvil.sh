#!/usr/bin/env bash
# Deploy MemoryAtlas to Anvil (Mac Studio M3 Ultra)
# Usage: ./scripts/deploy-to-anvil.sh [--with-data]
#
# Prerequisites:
#   - SSH access: ssh anvil (configured in ~/.ssh/config)
#   - Ollama running on Anvil with model pulled
#   - Python 3.11+ on Anvil

set -euo pipefail

ANVIL_HOST="anvil"
ANVIL_DIR="~/tools/memoryatlas"
LOCAL_DIR="$(cd "$(dirname "$0")/.." && pwd)"

echo "=== MemoryAtlas → Anvil Deploy ==="
echo "Local:  $LOCAL_DIR"
echo "Remote: $ANVIL_HOST:$ANVIL_DIR"
echo ""

# Step 1: Sync code (exclude data, venv, and local config)
echo "[1/5] Syncing code..."
rsync -avz --delete \
    --exclude '.venv/' \
    --exclude 'data/' \
    --exclude 'config.yaml' \
    --exclude '__pycache__/' \
    --exclude '*.pyc' \
    --exclude '*.egg-info/' \
    --exclude '.pytest_cache/' \
    "$LOCAL_DIR/" "$ANVIL_HOST:$ANVIL_DIR/"
echo "  Done."

# Step 2: Optionally sync data (database + transcripts, large!)
if [[ "${1:-}" == "--with-data" ]]; then
    echo "[2/5] Syncing data (this may take a while)..."
    rsync -avz --progress \
        --exclude 'cache/' \
        "$LOCAL_DIR/data/" "$ANVIL_HOST:$ANVIL_DIR/data/"
    echo "  Done."
else
    echo "[2/5] Skipping data sync (use --with-data to include)"
fi

# Step 3: Set up venv and install on Anvil
echo "[3/5] Setting up Python environment on Anvil..."
ssh "$ANVIL_HOST" bash -s <<'REMOTE_SETUP'
    cd ~/tools/memoryatlas

    # Create venv if missing
    if [ ! -d .venv ]; then
        python3 -m venv .venv
        echo "  Created .venv"
    fi

    source .venv/bin/activate

    # Install package in editable mode
    pip install -e . -q 2>&1 | tail -3

    # Create config from example if missing
    if [ ! -f config.yaml ]; then
        cp config.example.yaml config.yaml
        echo "  Created config.yaml from example — EDIT THIS for Anvil paths"
    fi

    # Create data dir
    mkdir -p data

    echo "  Python: $(python --version)"
    echo "  atlas: $(which atlas)"
REMOTE_SETUP
echo "  Done."

# Step 4: Verify Ollama on Anvil
echo "[4/5] Checking Ollama on Anvil..."
ssh "$ANVIL_HOST" bash -s <<'REMOTE_CHECK'
    if curl -s http://localhost:11434/api/tags > /dev/null 2>&1; then
        MODELS=$(curl -s http://localhost:11434/api/tags | python3 -c "import sys,json; print(', '.join(m['name'] for m in json.load(sys.stdin).get('models',[])))" 2>/dev/null || echo "parse error")
        echo "  OK  Ollama running. Models: $MODELS"
    else
        echo "  !!  Ollama not running. Start with: ollama serve"
    fi
REMOTE_CHECK

# Step 5: Run atlas status
echo "[5/5] Running atlas status on Anvil..."
ssh "$ANVIL_HOST" bash -s <<'REMOTE_STATUS'
    cd ~/tools/memoryatlas
    source .venv/bin/activate
    atlas status 2>&1 || echo "  (atlas status failed — config.yaml may need editing)"
REMOTE_STATUS

echo ""
echo "=== Deploy complete ==="
echo ""
echo "Next steps on Anvil:"
echo "  1. Edit ~/tools/memoryatlas/config.yaml for Anvil paths"
echo "  2. atlas doctor  (verify system health)"
echo "  3. atlas health   (verify Ollama + model)"
echo "  4. atlas polish --dry-run  (preview what will be processed)"
