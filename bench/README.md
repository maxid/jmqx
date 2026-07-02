# bench — 性能压测工具

提供 MQTT Broker 的一键性能对比脚本与参数模板，用于基准测试和回归检测。

## 脚本

### `compare.sh`

一键对比两次压测执行结果。默认参数：

| 参数 | 默认值 | 说明 |
|---|---|---|
| `bench.subscribers` | 10 | 订阅者数量 |
| `bench.publishers` | 1 | 发布者数量 |
| `bench.messages` | 5000 | 消息总数 |
| `bench.payloadBytes` | 32 | 消息负载大小 |
| `bench.port` | 18830 | MQTT 端口 |

```bash
# 使用默认参数对比
./bench/compare.sh

# 自定义参数
BENCH_SUBSCRIBERS=50 BENCH_PUBLISHERS=2 \
  BENCH_MESSAGES=20000 BENCH_PAYLOAD_BYTES=128 \
  ./bench/compare.sh
```

### 手动单次压测

```bash
mvn -pl jmqx-broker -Dtest=MqttLoadBenchmarkTest test \
  -Dbench.subscribers=10 \
  -Dbench.publishers=1 \
  -Dbench.messages=5000 \
  -Dbench.payloadBytes=32 \
  -Dbench.port=18830
```

## 压测用例

以下集成测试类由 `-Djmqx.integration.tests=true` 激活：

| 测试类 | 模块 | 说明 |
|---|---|---|
| `MassiveConnectionTest` | broker/cluster | 海量长连接稳定性测试 |
| `BrokerStressTest` | broker | 单节点消息吞吐压力测试 |
| `ClusterStressTest` | cluster | 集群消息吞吐压力测试 |
| `MqttLoadBenchmarkTest` | broker | 轻量级负载基准测试 |
| `DispatcherBenchmarkTest` | broker | 消息分发器基准测试 |

## 注意事项

- 若端口被占用，可修改 `bench.port` 参数
- 在 macOS 上可能需要 `netty-resolver-dns-native-macos` 依赖
- 对比前后建议保持一致硬件、JVM 参数和负载配置
