# HarData

基于 Rust 和 QUIC 的高性能数据传输服务。

## 特性

- **双协议传输**: QUIC + TCP，自动选择最优协议
- **CDC 去重**: 内容定义分块，弱哈希 (xxh3) + 强哈希 (BLAKE3)
- **智能压缩**: 自动识别文件类型，选择最优算法 (zstd/lz4/brotli)
- **三种同步模式**: `once` (增量)、`full` (全量)、`sync` (持续)
- **延迟队列**: sync 任务轮次间释放 worker，不阻塞其他任务
- **Web UI**: 实时任务监控面板
- **零拷贝**: Linux sendfile/splice 支持

## 快速开始

### 编译

```bash
cargo build --release
```

### 启动 Agent (数据源)

```bash
./target/release/hardata agent -c config.yaml
```

### 启动 Sync (数据目标)

```bash
./target/release/hardata sync -c config.yaml
```

### 配置

```yaml
sync:
  http_bind: "0.0.0.0:9080"
  data_dir: "./data/sync"
  web_ui: true
  regions:
    - name: "local"
      quic_bind: "127.0.0.1:9443"
      tcp_bind: "127.0.0.1:9444"

agent:
  quic_bind: "0.0.0.0:9443"
  tcp_bind: "0.0.0.0:9444"
  data_dir: "./data/agent"
```

## API

### 提交任务

```bash
curl -X POST http://127.0.0.1:9080/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "source_path": "/path/to/source",
    "dest_path": "./data/sync/dest",
    "region": "local",
    "job_type": "once",
    "priority": 5
  }'
```

### 任务类型

| 类型 | 说明 |
|------|------|
| `once` | 增量同步，跳过未变更文件 |
| `full` | 全量同步，强制处理所有文件 |
| `sync` | 持续同步，直到调用 `final` API |

### 查询任务

```bash
curl http://127.0.0.1:9080/api/v1/jobs
```

### 取消任务

```bash
curl -X POST http://127.0.0.1:9080/api/v1/jobs/{job_id}/cancel
```

### 结束 Sync 任务

```bash
curl -X POST http://127.0.0.1:9080/api/v1/jobs/{job_id}/final
```

## Web UI

启用 `web_ui: true` 后访问 `http://127.0.0.1:9080`

## 依赖

- Rust 1.75+
- SQLite (内嵌)

## 文档

- [架构设计](docs/architecture.md)

## 许可证

Apache-2.0
