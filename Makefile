# HarData Makefile

# Debug 构建
build:
	@echo "Debug 构建 (开发环境)..."
	cargo build
	@echo "构建完成: ./target/debug/hardata"

# Release 构建
build-release:
	@echo "Release 构建 (基础优化)..."
	cargo build --release
	@echo "构建完成: ./target/release/hardata"

# Linux 生产环境
build-linux-optimized:
	@echo "Linux 生产环境完整优化构建 (pread/pwrite + sendfile + SIMD)..."
	@echo "注意: 使用 Linux 系统调用优化"
	RUSTFLAGS="-C target-cpu=native -C opt-level=3" \
	cargo build --release --features io-uring
	@echo "构建完成: ./target/release/hardata"

# macOS 优化构建
build-macos-optimized:
	@echo "macOS 优化构建 (SIMD + Buffer Pool)..."
	@echo "注意: Linux 优化 I/O 在 macOS 上不可用"
	RUSTFLAGS="-C target-cpu=native -C opt-level=3" \
	cargo build --release
	@echo "构建完成: ./target/release/hardata"

# 前端构建
build-web:
	@echo "前端构建 (Dioxus WASM)..."
	@echo "清理旧的 web 构建产物..."
	rm -rf web/dist web/target
	mkdir -p web/dist
	cd web && dx build --release
	@echo "复制构建产物到 web/dist..."
	cp -r web/target/dx/hardata-web/release/web/public/* web/dist/
	@echo "复制 assets 静态资源..."
	cp -r web/assets/* web/dist/assets/ 2>/dev/null || true
	@echo "构建完成: ./web/dist/"

# 全量构建 (前端 + 后端)
build-all: build-web build-release
	@echo "全量构建完成"

# 全量优化构建 (前端 + 后端 macOS)
build-all-macos: build-web build-macos-optimized
	@echo "全量 macOS 优化构建完成"

# 全量优化构建 (前端 + 后端 Linux)
build-all-linux: build-web build-linux-optimized
	@echo "全量 Linux 优化构建完成"
