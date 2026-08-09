# HarData 工程脚本

这里存放不进入生产二进制的端到端验证与性能基准。所有入口均从仓库根目录以
Python 模块方式运行，公共能力集中在 `testkit`，场景代码不负责重复实现进程、
HTTP、测试数据和日志基础设施。

## 目录结构

```text
scripts/
├── benchmarks/
│   ├── loopback.py       # TCP/QUIC 基础传输与去重基准
│   └── stress.py         # 小文件批量与并发任务压测
├── consistency/
│   └── matrix.py         # 全场景数据一致性矩阵
├── testkit/
│   └── harness.py        # 进程、端口、HTTP、fixture、日志和完整性公共能力
└── tests/
    └── test_harness.py   # 公共能力快速单元测试
```

依赖方向固定为：

```text
benchmarks ─┐
            ├──► testkit
consistency ┘
```

`testkit` 不引用任何具体场景模块，避免测试入口之间形成隐式依赖。

## 稳定入口

```bash
make test-consistency
make perf-loopback
make perf-stress
make test-scripts
```

需要调整参数时直接运行模块：

```bash
python3 -m scripts.consistency.matrix --protocol tcp --skip-build
python3 -m scripts.benchmarks.loopback --rounds 3 --file-size-mib 256
python3 -m scripts.benchmarks.stress --small-file-count 2000 --concurrent-jobs 8
```

默认输出目录分别是 `/tmp/hardata-consistency-*`、`/tmp/hardata-perf-*` 和
`/tmp/hardata-stress-*`。传入 `--output /path/results.json` 时，结果 JSON、日志、
配置及本次测试数据会保留在指定文件的父目录。

## 责任边界

- `testkit/harness.py`：可复用基础设施，不定义业务测试场景。
- `consistency/matrix.py`：只负责正确性场景和断言，不统计性能结论。
- `benchmarks/loopback.py`：基础吞吐、Final 增量和去重收益。
- `benchmarks/stress.py`：小文件吞吐与多任务并发能力。
- `tests/test_harness.py`：公共 fixture、日志解析、配置与完整性断言的快速单元测试。

三个入口都启动真实的 release Agent/Sync 进程，并在退出时停止子进程。性能脚本
同时校验 SHA-256，避免把错误传输结果记录成有效性能数据。

测试运行期间产生的仓库级 `.hardata` TLS 临时状态会在进程退出后自动恢复或清理；
如果运行前已有该目录，原有内容会保留。
