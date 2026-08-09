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
make build-all
```

如果前端资源已经提前构建完成，也可以单独执行 `cargo build --release`。
干净 checkout 首次编译时，需要先执行 `make build-ui` 生成 Web UI 资源（`make build-web`
仍作为兼容别名保留）。

项目采用 Cargo workspace：应用层位于 `crates/hardata-app`，进程入口位于
`crates/hardata-bin`，终端命令与组合逻辑位于 `crates/hardata-terminal`，领域模型、协议和共享基础设施分别位于
`crates/hardata-domain`、`crates/hardata-protocol`、`crates/hardata-shared`，前端位于
`crates/hardata-ui`，也是 workspace member。workspace 默认 member 是 hardata-bin，因此现有的
`cargo build`、`cargo run` 命令无需改写。

命令定义和启动组合位于 `hardata-terminal`，`hardata-bin/src/main.rs` 只保留进程入口。
基础设施按职责拆分为 `hardata-infra-agent`、`hardata-infra-http`、
`hardata-infra-persistence`、`hardata-infra-transport`；`hardata-tool-cli` 提供独立的
差异检查工具。

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
  http_bind: "127.0.0.1:9080"
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

本地联调用 `127.0.0.1` 最简单；如果要把 `sync.http_bind` 暴露到非回环地址，必须额外配置 `sync.api_token`。

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
curl -X DELETE http://127.0.0.1:9080/api/v1/jobs/{job_id}
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
- [工程脚本说明](scripts/README.md)

## 数据一致性矩阵

执行真实 Agent/Sync 进程的全场景一致性验证：

```bash
make test-consistency
```

矩阵会分别覆盖 TCP 和 QUIC，并验证空文件、CDC 分块边界、二进制与重复数据、
嵌套目录/空目录、Unicode/空格/长文件名、符号链接、权限、目标类型切换、
增量/全量/最终轮、删除清理、include/exclude 过滤、幂等、Sync 重启恢复、取消、
Append/Tmp 落盘模式和路径安全拒绝。每个场景都比较文件类型、相对路径、权限、
大小、SHA-256、符号链接目标，并对普通文件做逐文件非浅层字节比较；结果会写入
`/tmp/hardata-consistency-*/results.json`。

测试过程中产生的仓库级 `.hardata` TLS 临时状态会在 Agent/Sync 退出后自动恢复或清理；
如果测试开始前已经存在 `.hardata`，其中的原有文件会保留。测试结果、日志和场景数据
仍按脚本说明保留在 `/tmp/hardata-*/` 输出目录中，便于复盘。

运行 Rust workspace 测试和脚本单元测试：

```bash
cargo test --workspace --all-targets --all-features --locked
make test-scripts
```

## 许可证

Apache-2.0
