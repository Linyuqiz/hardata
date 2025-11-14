#!/bin/bash
# Deploy hardata agent to remote server
# Usage: ./deploy-agent.sh [host] [remote_dir] [--skip-web]
#   host: SSH host alias (default: 4.48)
#   remote_dir: Remote directory (default: /home/psp_data)
#   --skip-web: Skip frontend build

set -e

# Activate mise environment if available (for zig, etc.)
if command -v mise &> /dev/null; then
    eval "$(mise activate bash --shims)"
fi

HOST="${1:-4.48}"
REMOTE_DIR="${2:-/home/psp_data}"
SKIP_WEB="${3}"
BINARY_NAME="hardata"
TARGET="x86_64-unknown-linux-gnu"

echo "=== Deploying hardata agent to $HOST:$REMOTE_DIR ==="

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

# Step 2: Cross compile for Linux with io-uring and CPU optimizations
echo "[2/5] Building backend for $TARGET with io-uring and CPU optimizations..."
if ! command -v cargo-zigbuild &> /dev/null; then
    echo "⚠️  cargo-zigbuild not found, using standard cargo build..."
    echo "   Install: cargo install cargo-zigbuild"
    RUSTFLAGS="-C target-cpu=x86-64-v3" cargo build --release --target $TARGET --package hardata --features io-uring
else
    RUSTFLAGS="-C target-cpu=x86-64-v3" cargo zigbuild --release --target $TARGET --package hardata --features io-uring
fi

# Step 3: Stop old agent on remote
echo "[3/5] Stopping old agent on $HOST..."
ssh $HOST "pkill -f 'hardata agent'" 2>/dev/null || true
sleep 1

# Step 4: Copy binary to remote
echo "[4/5] Copying binary to $HOST:$REMOTE_DIR..."
scp ./target/$TARGET/release/$BINARY_NAME $HOST:$REMOTE_DIR/$BINARY_NAME

# Step 5: Start new agent
echo "[5/5] Starting agent on $HOST..."
ssh -f $HOST "cd $REMOTE_DIR && nohup ./$BINARY_NAME agent -c config.yaml > agent.log 2>&1 &"
sleep 2

# Verify
echo ""
echo "=== Verifying deployment ==="
ssh $HOST "ps aux | grep 'hardata agent' | grep -v grep || echo 'Warning: agent process not found'"
ssh $HOST "tail -5 $REMOTE_DIR/agent.log 2>/dev/null || echo 'No log yet'"

echo ""
echo "=== Deployment complete ==="
