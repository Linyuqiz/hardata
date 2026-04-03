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

# Linux 生产环境（本机 Linux 构建，优化参数由 .cargo/config.toml 统一管理）
build-linux-optimized:
	@echo "Linux 生产环境构建 (io-uring + SIMD)..."
	cargo build --release --features io-uring
	@echo "构建完成: ./target/release/hardata"

# Linux 交叉编译（macOS → Linux x86_64，需要 cargo-zigbuild）
build-linux-cross:
	@echo "交叉编译 Linux x86_64 (io-uring + SIMD)..."
	cargo zigbuild --release --target x86_64-unknown-linux-gnu --features io-uring
	@echo "构建完成: ./target/x86_64-unknown-linux-gnu/release/hardata"

# macOS 优化构建（优化参数由 .cargo/config.toml 统一管理）
build-macos-optimized:
	@echo "macOS 优化构建 (SIMD + native CPU)..."
	cargo build --release
	@echo "构建完成: ./target/release/hardata"

# 前端构建
build-web:
	@echo "前端构建 (Dioxus WASM)..."
	rm -rf web/dist web/target
	mkdir -p web/dist
	cd web && dx build --release
	cp -r web/target/dx/hardata-web/release/web/public/* web/dist/
	cp -r web/assets/* web/dist/assets/ 2>/dev/null || true
	@echo "构建完成: ./web/dist/"

# 全量构建 (前端 + 后端)
build-all: build-web build-release
	@echo "全量构建完成"

# 全量优化构建 (前端 + 后端 macOS)
build-all-macos: build-web build-macos-optimized
	@echo "全量 macOS 优化构建完成"

# 全量优化构建 (前端 + 后端 Linux 本机)
build-all-linux: build-web build-linux-optimized
	@echo "全量 Linux 优化构建完成"

# 全量交叉编译 (前端 + macOS → Linux x86_64)
build-all-linux-cross: build-web build-linux-cross
	@echo "全量 Linux 交叉编译完成: ./target/x86_64-unknown-linux-gnu/release/hardata"

# 清理构建产物
clean:
	cargo clean
	rm -rf web/dist web/target
	@echo "清理完成"

# 本地回环性能基准
perf-loopback:
	@echo "执行本地回环性能基准 (TCP + QUIC)..."
	python3 scripts/perf_loopback.py

# 生产侧补充压测
perf-stress:
	@echo "执行小文件与并发补充压测 (TCP + QUIC)..."
	python3 scripts/perf_stress.py
