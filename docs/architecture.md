# HarData 架构设计

## 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│                         Sync Node                           │
│  ┌─────────┐  ┌───────────┐  ┌──────────┐  ┌─────────────┐ │
│  │ HTTP API│  │ Scheduler │  │ Workers  │  │DelayedQueue │ │
│  └────┬────┘  └─────┬─────┘  └────┬─────┘  └──────┬──────┘ │
│       │             │             │               │         │
│       └─────────────┴─────────────┴───────────────┘         │
│                           │                                  │
│                    ┌──────┴──────┐                          │
│                    │ QUIC / TCP  │                          │
│                    └──────┬──────┘                          │
└───────────────────────────┼─────────────────────────────────┘
                            │
┌───────────────────────────┼─────────────────────────────────┐
│                    ┌──────┴──────┐                          │
│                    │ QUIC / TCP  │                          │
│                    └──────┬──────┘                          │
│                           │                                  │
│  ┌────────────────────────┴────────────────────────────────┐│
│  │                    Agent Server                          ││
│  │  ┌──────────┐  ┌────────────┐  ┌───────────────────────┐││
│  │  │ File I/O │  │ Compression│  │ Block Transfer        │││
│  │  └──────────┘  └────────────┘  └───────────────────────┘││
│  └──────────────────────────────────────────────────────────┘│
│                         Agent Node                           │
└─────────────────────────────────────────────────────────────┘
```

## 模块结构

```
Cargo.toml                     # virtual workspace 根配置与统一依赖
├── crates/
│   ├── hardata-bin/            # 最小进程入口（main.rs）
│   ├── hardata-terminal/       # CLI 命令与运行时组合
│   │   └── src/bootstrap/      # sync/agent 启动组合与索引扫描
│   ├── hardata-app/            # 应用服务、同步用例、端口契约
│   │   └── src/application/    # scheduler、use_cases、ports
│   ├── hardata-domain/         # Job、Chunk、TransferState 等领域模型
│   ├── hardata-protocol/       # wire message 与 codec
│   ├── hardata-shared/         # CDC、压缩、文件、错误等通用能力
│   ├── hardata-infra-agent/    # Agent 计算与 TCP/QUIC 入站适配器
│   ├── hardata-infra-http/     # HTTP API、静态 UI 入站适配器
│   ├── hardata-infra-persistence/ # SQLite 持久化适配器
│   ├── hardata-infra-transport/   # TCP/QUIC 出站传输适配器
│   ├── hardata-tool-cli/       # diff/manifest 等 CLI 工具适配器
│   └── hardata-ui/             # Dioxus 前端 workspace member
```

`hardata-bin` 是唯一进程入口，`main.rs` 只负责 allocator、tokio 入口和错误退出；CLI 与具体
启动流程位于 `hardata-terminal`。`hardata-app` 不再承载 HTTP、Agent、SQLite 或 TCP/QUIC
实现，这些实现分别位于 `hardata-infra-*` / `hardata-tool-*` crate。应用端口集中在
`hardata-app/src/application/ports.rs`，旧模块路径只保留薄兼容门面。

## Clean Architecture 边界

依赖只能由外向内：

```text
inbound adapters (HTTP / CLI / Agent Server)
                    │
                    ▼
application (use cases / scheduler / ports)
                    │
                    ▼
domain (jobs / chunks / transfer state)

outbound adapters (SQLite / Sled / filesystem / TCP / QUIC)
                    ▲
                    │
             application ports
```

各层职责：

- `domain`：业务实体和值对象，不创建 HTTP Router、数据库连接池或网络客户端。
- `application`：同步用例、调度编排和端口契约。HTTP 通过
  `application::use_cases::JobUseCases` 调用应用层，持久化通过
  `application::ports::TransferStateStore` 反转依赖。
- `hardata-infra-http` / `hardata-tool-cli`：入站适配器，将 HTTP/CLI 输入转换为用例调用。
- `hardata-infra-agent`：Agent 的文件计算、协议处理和 TCP/QUIC 入站服务。
- `hardata-infra-persistence` / `hardata-infra-transport`：SQLite 与 TCP/QUIC 出站适配器；Sled
  chunk index 由应用编排层管理。
- `hardata-terminal/src/bootstrap`：读取配置、组装依赖并启动节点，不承载业务规则。
- `hardata-terminal/src/bootstrap/index_scan.rs`：后台全局索引扫描，与服务启动生命周期解耦。
- `hardata-protocol`：独立维护 wire message 与 codec，避免传输协议定义领域模型。
- `hardata-shared`：提供多个 crate 共用的基础能力，不依赖 runtime。

Workspace 的 crate 依赖关系：

```text
hardata-bin ──► hardata-terminal
hardata-terminal
├── hardata-app ──► hardata-domain / hardata-protocol / hardata-shared
│                  └─► hardata-infra-persistence / hardata-infra-transport (兼容门面)
├── hardata-infra-agent ──► hardata-protocol / hardata-shared
├── hardata-infra-http ──► hardata-app
├── hardata-infra-persistence ──► hardata-domain / hardata-shared
├── hardata-infra-transport ──► hardata-protocol / hardata-shared
└── hardata-tool-cli ──► hardata-app / hardata-shared

同步调度器通过兼容门面使用 infra 类型（仅为保持现有调度 API 稳定）；新用例通过
`application::ports` 编程，基础设施实现不反向依赖 app。

hardata-protocol ──► hardata-shared
hardata-ui（独立前端 workspace member）
```

`hardata-terminal` 是服务组合根，`hardata-bin` 只是最终可执行入口；`hardata-app` 提供应用能力。根 workspace 将
`hardata-bin` 设为 `default-members`，因此根目录执行 `cargo build`、`cargo run` 和 `cargo test`
仍默认操作服务端。所有依赖版本与 features 统一定义在根 `Cargo.toml` 的
`[workspace.dependencies]`，成员 crate 只通过 `workspace = true` 声明直接依赖。

组合根只有两个启动模式，均位于 `hardata-terminal`：

- `crates/hardata-terminal/src/bootstrap/sync.rs`：组装 Sync 调度器、持久化、索引和 HTTP 适配器。
- `crates/hardata-terminal/src/bootstrap/agent.rs`：组装 Agent 计算服务及 TCP/QUIC 入站适配器。

此次结构调整不改变公开 wire protocol 和 SQLite schema。新增业务行为应进入
`domain` 或 `application`，适配器只负责协议转换和外部 I/O。

## 运行时状态与测试隔离

`.hardata` 是运行时状态目录，不属于任何一个 crate 的源代码。默认同步数据、元数据和
chunk 缓存使用 `.hardata/...` 路径，生产 Agent 的 TLS 材料位于进程工作目录下的
`.hardata/tls`。这些相对路径按进程当前工作目录解析，因此不应在 `crates/*` 下手工维护
多份目录；从不同工作目录启动程序会产生不同的运行时目录。

测试会将这类状态视为临时产物：Rust 测试通过作用域清理器回收测试创建的 TLS 父目录，
工程脚本的 `testkit.RuntimeStateGuard` 会在 Agent/Sync 停止后恢复测试前已有内容，或删除
本次测试新建的仓库级 `.hardata`。测试结果、日志和场景数据仍保留在 `/tmp/hardata-*/`，
用于失败复盘；`.hardata` 目录由 `.gitignore` 忽略，不应提交到版本库。

## 日志规范

关键链路使用结构化 `tracing` 事件，统一字段如下：

- `operation`：稳定的点号命名事件，例如 `job.round_started`、`transport.connection_failed`。
- `job_id`、`region`、`round_id`：任务定位字段；任务相关事件尽量全部携带。
- `status`、`reason`、`protocol`、`error`：状态转换、跳过原因、协议选择和失败原因。

`info` 记录状态转换和生命周期，`warn` 记录可恢复降级或清理失败，`error` 记录请求或任务失败，`debug` 只记录高频诊断细节。日志消息保持短句，查询优先使用字段；不记录 API token、私钥和完整文件内容。

## 核心组件

### 1. Scheduler (调度器)

负责任务调度和 Worker 管理。

```
┌────────────────────────────────────────────────────┐
│                   Scheduler                         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────┐ │
│  │ PriorityQueue│  │ DelayedQueue │  │ Workers  │ │
│  │   (待执行)    │  │  (延迟执行)   │  │  (执行)  │ │
│  └──────┬───────┘  └───────┬──────┘  └────┬─────┘ │
│         │                  │               │       │
│         └──────────────────┴───────────────┘       │
└────────────────────────────────────────────────────┘
```

### 2. Worker 循环

```
Worker 启动
    │
    ▼
┌─────────────────┐
│ 从队列取任务     │◄──────────────────┐
└────────┬────────┘                   │
         │                            │
         ▼                            │
┌─────────────────┐                   │
│ 执行任务轮次     │                   │
└────────┬────────┘                   │
         │                            │
         ▼                            │
    ┌────┴────┐                       │
    │ 任务类型 │                       │
    └────┬────┘                       │
         │                            │
    ┌────┼────┐                       │
    ▼    ▼    ▼                       │
  once  full sync                     │
    │    │    │                       │
    ▼    ▼    │                       │
 完成  完成   │                       │
              ▼                       │
       ┌─────────────┐                │
       │ 放入延迟队列 │                │
       └──────┬──────┘                │
              │                       │
              ▼                       │
       ┌─────────────┐                │
       │ 释放 Worker  │────────────────┘
       └─────────────┘
```

### 3. DelayedQueue (延迟队列)

解决 sync 任务占用 Worker 问题。

```rust
pub struct DelayedQueue<T> {
    items: Mutex<BTreeMap<Instant, Vec<T>>>,
}
```

- `insert(run_at, job)`: 插入延迟任务
- `pop_ready()`: 取出所有到期任务
- 延迟调度器每 100ms 检查，将到期任务重新入队

### 4. 去重机制

```
文件 ──► CDC 分块 ──► 弱哈希 (xxh3) ──► 本地查找
                          │
                          ▼
                     ┌────┴────┐
                     │ 命中？   │
                     └────┬────┘
                          │
              ┌───────────┼───────────┐
              ▼           ▼           ▼
            未命中       命中        冲突
              │           │           │
              ▼           ▼           ▼
           传输块     跳过传输    强哈希验证
                                     │
                              ┌──────┴──────┐
                              ▼             ▼
                            相同          不同
                              │             │
                              ▼             ▼
                          跳过传输       传输块
```

### 5. 协议选择

```
┌─────────────────────────────────────────┐
│           ProtocolSelector              │
│  ┌─────────────┐    ┌─────────────────┐ │
│  │ QUIC 优先   │    │ 失败降级 TCP    │ │
│  └──────┬──────┘    └────────┬────────┘ │
│         │                    │          │
│         └────────┬───────────┘          │
│                  ▼                      │
│         ┌───────────────┐               │
│         │ 延迟/吞吐评估  │               │
│         └───────────────┘               │
└─────────────────────────────────────────┘
```

## 数据流

### 同步流程

```
1. 用户提交 Job
       │
       ▼
2. Scheduler 入队
       │
       ▼
3. Worker 取任务
       │
       ▼
4. 扫描远程目录 (Agent)
       │
       ▼
5. 稳定性过滤 (SizeFreezer)
       │
       ▼
6. 变更检测 (size + mtime)
       │
       ▼
7. CDC 分块 + 去重
       │
       ▼
8. 传输变更块
       │
       ▼
9. 本地写入
       │
       ▼
10. 更新缓存
```

### 压缩策略

| 文件类型 | 算法 | 原因 |
|---------|------|------|
| 文本/JSON/XML | Brotli | 高压缩比 |
| 日志/临时文件 | LZ4 | 极速 |
| 通用二进制 | Zstd | 平衡 |
| 已压缩文件 | 无 | 跳过 |

## 性能优化

### 内存

- mimalloc 高性能分配器
- 内存映射 (mmap) 大文件处理
- DashMap 无锁并发缓存

### I/O

- Linux: sendfile/splice 零拷贝
- 批量传输减少系统调用
- 连接池复用

### 网络

- QUIC 多路复用
- 自适应并发控制 (AIMD)
- 智能重试策略

## 配置参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| max_concurrent_jobs | 32 | 最大并发任务数 |
| scan_interval | 10s | sync 扫描间隔 |
| chunk_size | 64KB-1MB | CDC 分块大小范围 |
| cache_ttl | 3600s | 文件缓存过期时间 |
| cache_max_entries | 100000 | 最大缓存条目 |
