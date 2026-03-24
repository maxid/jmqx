## 压测模板与对比脚本

此目录提供一键对比前后性能的脚本与参数模板。

### 1. 基准脚本（对比两次执行）

```bash
./bench/compare.sh
```

默认参数：
- `bench.subscribers=10`
- `bench.publishers=1`
- `bench.messages=5000`
- `bench.payloadBytes=32`
- `bench.port=18830`

你可以通过环境变量覆盖：

```bash
BENCH_SUBSCRIBERS=50 BENCH_PUBLISHERS=2 BENCH_MESSAGES=20000 BENCH_PAYLOAD_BYTES=128 ./bench/compare.sh
```

### 2. 手动执行单次压测

```bash
mvn -pl jmqx-broker -Dtest=MqttLoadBenchmarkTest test \
  -Dbench.subscribers=10 \
  -Dbench.publishers=1 \
  -Dbench.messages=5000 \
  -Dbench.payloadBytes=32 \
  -Dbench.port=18830
```

### 3. 注意事项

- 若端口被占用，可修改 `bench.port`。
- 运行在 macOS 时，可能需要安装 `io.netty:netty-resolver-dns-native-macos`，否则会有 DNS 相关告警（不影响压测结果）。
- 对比前后建议保证相同硬件、JVM 参数与负载。
