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
build-ui:
	@echo "前端构建 (Dioxus WASM)..."
	rm -rf crates/hardata-ui/dist target/dx/hardata-ui
	mkdir -p crates/hardata-ui/dist
	dx build --release --package hardata-ui --locked --debug-symbols false
	cp -r target/dx/hardata-ui/release/web/public/* crates/hardata-ui/dist/
	cp -r crates/hardata-ui/assets/* crates/hardata-ui/dist/assets/ 2>/dev/null || true
	@echo "构建完成: ./crates/hardata-ui/dist/"

# 兼容旧命令；新结构统一称为 UI。
build-web: build-ui

# 全量构建 (前端 + 后端)
build-all: build-ui build-release
	@echo "全量构建完成"

# 全量优化构建 (前端 + 后端 macOS)
build-all-macos: build-ui build-macos-optimized
	@echo "全量 macOS 优化构建完成"

# 全量优化构建 (前端 + 后端 Linux 本机)
build-all-linux: build-ui build-linux-optimized
	@echo "全量 Linux 优化构建完成"

# 全量交叉编译 (前端 + macOS → Linux x86_64)
build-all-linux-cross: build-ui build-linux-cross
	@echo "全量 Linux 交叉编译完成: ./target/x86_64-unknown-linux-gnu/release/hardata"

# 清理构建产物
clean:
	cargo clean
	rm -rf crates/hardata-ui/dist
	@echo "清理完成"

# 本地回环性能基准
perf-loopback:
	@echo "执行本地回环性能基准 (TCP + QUIC)..."
	python3 -m scripts.benchmarks.loopback

# 生产侧补充压测
perf-stress:
	@echo "执行小文件与并发补充压测 (TCP + QUIC)..."
	python3 -m scripts.benchmarks.stress

# 全场景数据一致性矩阵
test-consistency:
	@echo "执行全场景数据一致性矩阵 (TCP + QUIC)..."
	python3 -m scripts.consistency.matrix

# 工程脚本公共能力单元测试
test-scripts:
	@echo "执行工程脚本单元测试..."
	python3 -m unittest discover -s scripts/tests -t .
