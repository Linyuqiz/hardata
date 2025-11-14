#!/bin/bash
# Deploy hardata sync service locally
# Usage: ./deploy-sync.sh [config_file] [--skip-web]
#   config_file: Config file path (default: config.yaml)
#   --skip-web: Skip frontend build

set -e

CONFIG="${1:-config.yaml}"
SKIP_WEB="${2}"
BINARY_NAME="hardata"
LOG_FILE="sync.log"

echo "=== Deploying hardata sync service locally ==="

# Step 1: Build frontend (unless skipped)
if [ "$SKIP_WEB" != "--skip-web" ]; then
    echo "[1/5] Building frontend (Dioxus WASM)..."
    echo "Cleaning old web build artifacts..."
    rm -rf web/dist web/target
    mkdir -p web/dist
    cd web
    if ! dx build --release; then
        echo "⚠️  Frontend build failed, continuing with backend only..."
        echo "   Run 'cd web && dx build --release' manually to debug"
    else
        echo "✓ Frontend built successfully"
        # Copy build artifacts to web/dist
        if [ -d "target/dx/hardata-web/release/web/public" ]; then
            cp -r target/dx/hardata-web/release/web/public/* dist/
            # Copy assets (CSS, etc.)
            if [ -d "assets" ]; then
                cp -r assets/* dist/assets/ 2>/dev/null || true
            fi
            echo "✓ Copied build artifacts to web/dist/"
        else
            echo "⚠️  Build output directory not found"
        fi
    fi
    cd ..
else
    echo "[1/5] Skipping frontend build (--skip-web)"
fi

# Step 2: Build backend with optimizations
echo "[2/5] Building backend release binary with optimizations..."
RUSTFLAGS="-C target-cpu=native" cargo build --release --package hardata

# Step 3: Stop old sync service
echo "[3/5] Stopping old sync service..."
pkill -f 'hardata sync' 2>/dev/null || true
sleep 1

# Step 4: Verify config exists
echo "[4/5] Checking config file..."
if [ ! -f "$CONFIG" ]; then
    echo "Error: Config file '$CONFIG' not found"
    exit 1
fi

# Step 5: Start new sync service
echo "[5/5] Starting sync service..."
nohup ./target/release/$BINARY_NAME sync -c "$CONFIG" > "$LOG_FILE" 2>&1 &
sleep 2

# Verify
echo ""
echo "=== Verifying deployment ==="
if pgrep -f "hardata sync" > /dev/null; then
    echo "Sync service is running (PID: $(pgrep -f 'hardata sync'))"
else
    echo "Warning: sync process not found"
fi

echo ""
echo "=== Recent logs ==="
tail -10 "$LOG_FILE" 2>/dev/null || echo "No log yet"

echo ""
echo "=== Deployment complete ==="
echo "Log file: $LOG_FILE"
echo "To view logs: tail -f $LOG_FILE"
