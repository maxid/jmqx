# Jmqx 开源的轻量级 MQTT 消息代理 Broker

Jmqx lightweight MQTT Broker

Jmqx 是在 [SMQTT 1.x](https://github.com/quickmsg/smqtt) 基础上的重构版本，主要是对 MQTT 实现部分的重构，并修正了一些问题，SMQTT 是作为一个独立的完整应用，而 Jmqx 的目标是作为一个库栈嵌入到用户应用中，为用户应用提供 MQTT 设备接入能力，实现自己的物联网平台，在此感谢 SMQTT 作者开源了那么优秀的项目。

## 相同点：

1. 支持 MQTT v3.1、v3.1.1、v5 协议（v5不完整实现）
2. 支持集群

## 不同点：

1. 不提供消息持久化（只提供内存实现，若需要可参考 SMQTT 或 SMQTTX 的实现）
2. 不提供规则引擎
3. 不提供读取配置文件（参考 jmqx-example 注入配置）

## 改进点：

1. 可同时支持 MQTT、MQTTS、MQTT-WS、MQTT-WSS 端口监听，方便不同需求的设备接入
2. 提供构造器注入用户自定义设备鉴权管理、主题访问控制管理、设备生命周期监听模块，方便与spring boot等框架集成
3. 方便作为库栈嵌入用户应用（参考 jmqx-example）
4. 修复了订阅消息QoS、Retained状态错误、Retain消息重发、消息顺序异常等问题
5. 同一进程内可以通过设置不同命名空间启动多个实例（2026-1-11）
6. 增加 PortUtil 在端口占用时自动获取新端口（2026-1-14）
7. 从 1.4.7 开始升级至 JDK 17, 性能有所提升, m1 下约 12w msg/s（2026-3-25）
8. 修复发布及订阅未授权主题时, 未按 MQTT V3/V5 正确处理 ACK 消息（2026-4-1）
9. 重要：修复因在 Netty Event Loop 线程中调用鉴权（鉴权业务由用户实现，不确定会采用何种方案）可能会发阻塞，而影响心跳、收发包和重连风暴（2026-4-17)
10. 重要：修复 PUBREL 流程不当使用 MessageUtils.wrapPublishMessage 产生 Netty ByteBuf 泄漏（2026-4-18)
11. 增强：支持通过 clientId 主动向指定设备下发消息（在设备订阅同一个主题情况下），不经过主题路由，走完整分发管线（含 ACL 检查），集群模式自动扩散（2026-6-24）
12. 测试：同一 JVM 内可通过 namespace + node 组合键启动多个集群节点，支持集成测试验证集群间消息路由（2026-6-25）
13. 测试：连接压力测试与消息压力测试独立为 `MassiveConnectionTest`、`BrokerStressTest`、`ClusterStressTest`（2026-6-27）
## 使用示例

- 单元测试方式启动：单节点

引入依赖

```xml
        <dependency>
            <groupId>plus.jmqx.iot</groupId>
            <artifactId>jmqx-broker</artifactId>
            <version>1.4.12</version>
        </dependency>
```

编写测试用例

```java
/**
 * MQTT Broker 测试用例
 */
@Slf4j
class BootstrapTest {
    @Test
    void brokerTest() throws Exception {
        // 日志配置（可选）
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        loggerContext.getLogger("root").setLevel(Level.INFO);
        loggerContext.getLogger("reactor.netty").setLevel(Level.INFO);
        loggerContext.getLogger("plus.jmqx.broker").setLevel(Level.INFO);
        loggerContext.getLogger("plus.jmqx.broker.mqtt.message.impl").setLevel(Level.DEBUG);
        // 构建配置信息
        MqttConfiguration config = new MqttConfiguration();
        // 设置启用SSL（可选）
        config.setSslEnable(true);
        config.setSslCa(Objects.requireNonNull(BootstrapTest.class.getResource("/ca.crt")).getPath());
        config.setSslCrt(Objects.requireNonNull(BootstrapTest.class.getResource("/server.crt")).getPath());
        config.setSslKey(Objects.requireNonNull(BootstrapTest.class.getResource("/server.key")).getPath());
        // 构建器注入配置信息（必选）及设备生命周期订阅器（可选）
        Bootstrap bootstrap = new Bootstrap(config, new PlatformDispatcher() {
            @Override
            public Mono<Void> onConnect(ConnectMessage message) {
                return Mono.fromRunnable(() -> {
                    log.info("设备上线：{}", message);
                });
            }

            @Override
            public Mono<Void> onDisconnect(DisconnectMessage message) {
                return Mono.fromRunnable(() -> {
                    log.info("设备断开连接：{}", message);
                });
            }

            @Override
            public Mono<Void> onConnectionLost(ConnectionLostMessage message) {
                return Mono.fromRunnable(() -> {
                    log.info("设备离线：{}", message);
                });
            }

            @Override
            public Mono<Void> onPublish(PublishMessage message) {
                return Mono.fromRunnable(() -> {
                    log.info("设备上报消息：PublishMessage(clientId={}, username={}, topic={}, payload={})",
                            message.getClientId(),
                            message.getUsername(),
                            message.getTopic(),
                            new String(message.getPayload(), StandardCharsets.UTF_8));
                });
            }
        });
        bootstrap.start().block();
        Thread.sleep(3600 * 1000);
        bootstrap.shutdown();
    }
}
```

启动测试用例控制台输出

```shell
14:13:53.734 [jmqx-event-loop-select-nio-2] INFO plus.jmqx.broker.mqtt.transport.impl.MqttTransport - mqtt broker start success host 0:0:0:0:0:0:0:0 port 1883
14:13:53.901 [jmqx-event-loop-select-nio-3] INFO plus.jmqx.broker.mqtt.transport.impl.MqttTransport - mqtts broker start success host 0:0:0:0:0:0:0:0 port 8883
14:13:53.902 [jmqx-event-loop-select-nio-4] INFO plus.jmqx.broker.mqtt.transport.impl.MqttTransport - mqtt-ws broker start success host 0:0:0:0:0:0:0:0 port 1884
14:13:53.906 [jmqx-event-loop-select-nio-5] INFO plus.jmqx.broker.mqtt.transport.impl.MqttTransport - mqtt-wss broker start success host 0:0:0:0:0:0:0:0 port 8884
```

## 压力测试

集成测试默认不运行，需加 `-Djmqx.integration.tests=true`。压测类已从 `BootstrapTest` 拆出，分为**连接压力测试**（海量长连接）与**消息压力测试**（高吞吐 PUBLISH）。

### 连接压力测试（MassiveConnectionTest）

验证 Broker 在大量 MQTT 长连接下的稳定性：连接建立、会话计数、保活、关闭后无泄漏。客户端使用 `cleanSession=true`，定时发送 `PINGREQ` 保活，关闭时发送 `DISCONNECT`。

| 系统属性 | 默认值 | 说明 |
|---|---|---|
| `jmqx.test.targetConnections` | `10000` | 单节点目标连接数 |
| `jmqx.test.targetConnectionsPerNode` | `5000` | 集群每节点目标连接数 |
| `jmqx.test.connectConcurrency` | `100`（单节点）/ `50`（集群） | 每批并发建连数 |
| `jmqx.test.batchIntervalMs` | `10` | 批次间隔（毫秒） |
| `jmqx.test.holdSeconds` | `60`（单节点）/ `30`（集群） | 保持连接时长（秒），`0` 表示不保持 |
| `jmqx.test.port` | `0` | MQTT 端口，`0` 表示自动分配空闲端口 |
| `jmqx.test.maxConnections` | `0` | 连接上限，`0` 表示不限制（用于验证准入机制） |
| `jmqx.test.connectTimeoutSeconds` | `30` | 单次建连超时（秒） |
| `jmqx.test.keepAliveSeconds` | `60` | 客户端 keepalive（秒） |

**单节点大量连接**

```shell
mvn test -pl jmqx-broker \
  -Djmqx.integration.tests=true \
  -Dtest=MassiveConnectionTest#testMassiveConnectionsSingleNode \
  -Djmqx.test.targetConnections=5000 \
  -Djmqx.test.connectConcurrency=50
```

**集群双节点大量连接**（`jmqx-cluster` 模块，node-1 端口 1883，node-2 端口 2883）

```shell
mvn test -pl jmqx-cluster \
  -Djmqx.integration.tests=true \
  -Dtest=MassiveConnectionTest#testMassiveConnectionsCluster \
  -Djmqx.test.targetConnectionsPerNode=5000 \
  -Djmqx.test.connectConcurrency=50
```

验证项包括：`SessionRegistry.counts()` 与成功连接数一致、保持连接期间无 OOM、全部断开后会话归零、同 `clientId` 可重连（无泄漏）。

### 消息压力测试（BrokerStressTest / ClusterStressTest）

多客户端循环发布 QoS1 消息，统计 `published`、`acked`、`dispatchReceived` 及区间吞吐。启动前执行预检发布，确认 PUBACK 与分发器回调均正常。

| 系统属性 | 默认值 | 说明 |
|---|---|---|
| `jmqx.stress.port` | `1883` | node-1 MQTT 端口（集群 node-2 为 `port + 1000`） |
| `jmqx.stress.threads` | `4` | 压测客户端数（集群均分两节点） |
| `jmqx.stress.durationSeconds` | `600` | 每客户端持续发布时长（秒） |
| `jmqx.stress.payloadBytes` | `64` | 消息负载大小（字节） |
| `jmqx.stress.flushEvery` | `256` | 每 N 条消息 flush 一次 |
| `jmqx.stress.inFlightLimit` | `20000` | 单客户端飞行窗口上限 |
| `jmqx.stress.timeoutSeconds` | `720` | 整体超时（秒） |
| `jmqx.stress.reportIntervalSeconds` | `10` | 进度日志间隔（秒） |
| `jmqx.stress.connectConcurrency` | `50` | 集群压测逐批建连并发数 |
| `jmqx.stress.connectTimeoutSeconds` | `30` | 单次建连超时（秒） |
| `jmqx.stress.clusterPort1` | `7771` | 集群 node-1 通信端口 |
| `jmqx.stress.clusterPort2` | `7772` | 集群 node-2 通信端口 |

`jmqx.stress.port` 设为 `0` 时由 OS 自动分配 MQTT 端口；大于 `0` 时若端口被占用，测试启动前会解析为下一个可用端口，客户端与 Broker 使用同一端口，避免 `Connection refused`。

**单节点消息吞吐**

```shell
MAVEN_OPTS="-Xmx4g" mvn test -pl jmqx-broker \
  -Djmqx.integration.tests=true \
  -Dtest=BrokerStressTest \
  -Djmqx.stress.durationSeconds=600 \
  -Djmqx.stress.threads=200 \
  -Djmqx.stress.reportIntervalSeconds=5
```

```shell
23:10:06.667 [jmqx-event-loop-select-nio-2] INFO plus.jmqx.broker.mqtt.transport.impl.MqttTransport - mqtt broker start success host 0:0:0:0:0:0:0:0 port 1883
23:10:11.683 [pool-7-thread-1] INFO plus.jmqx.broker.support.StressTestSupport - broker stress progress: threads=200, published=4083809, acked=88144, dispatchReceived=90765, intervalThroughput=17612 msg/s, elapsed=5.0s
23:10:16.673 [pool-7-thread-1] INFO plus.jmqx.broker.support.StressTestSupport - broker stress progress: threads=200, published=4228594, acked=357965, dispatchReceived=367494, intervalThroughput=53962 msg/s, elapsed=10.0s
23:10:21.673 [pool-7-thread-1] INFO plus.jmqx.broker.support.StressTestSupport - broker stress progress: threads=200, published=4560826, acked=1062562, dispatchReceived=1065149, intervalThroughput=140927 msg/s, elapsed=15.0s
23:10:26.680 [pool-7-thread-1] INFO plus.jmqx.broker.support.StressTestSupport - broker stress progress: threads=200, published=5356186, acked=2012433, dispatchReceived=2028850, intervalThroughput=190019 msg/s, elapsed=20.0s
23:10:31.672 [pool-7-thread-1] INFO plus.jmqx.broker.support.StressTestSupport - broker stress progress: threads=200, published=6309925, acked=2638320, dispatchReceived=2697606, intervalThroughput=125224 msg/s, elapsed=25.0s
...
23:15:31.673 [pool-7-thread-1] INFO plus.jmqx.broker.support.StressTestSupport - broker stress progress: threads=200, published=43563399, acked=39798091, dispatchReceived=39798153, intervalThroughput=121112 msg/s, elapsed=325.0s
23:15:36.675 [pool-7-thread-1] INFO plus.jmqx.broker.support.StressTestSupport - broker stress progress: threads=200, published=44166253, acked=40411317, dispatchReceived=40411366, intervalThroughput=122616 msg/s, elapsed=330.0s
23:15:41.689 [pool-7-thread-1] INFO plus.jmqx.broker.support.StressTestSupport - broker stress progress: threads=200, published=44730782, acked=40990637, dispatchReceived=40991666, intervalThroughput=115508 msg/s, elapsed=335.0s
23:15:46.672 [pool-7-thread-1] INFO plus.jmqx.broker.support.StressTestSupport - broker stress progress: threads=200, published=45343519, acked=41630082, dispatchReceived=41631308, intervalThroughput=128340 msg/s, elapsed=340.0s
23:15:51.672 [pool-7-thread-1] INFO plus.jmqx.broker.support.StressTestSupport - broker stress progress: threads=200, published=45971698, acked=42241166, dispatchReceived=42241476, intervalThroughput=122201 msg/s, elapsed=345.0s
23:15:56.671 [pool-7-thread-1] INFO plus.jmqx.broker.support.StressTestSupport - broker stress progress: threads=200, published=46599389, acked=42849953, dispatchReceived=42850681, intervalThroughput=121783 msg/s, elapsed=350.0s
...
23:19:46.669 [pool-7-thread-1] INFO plus.jmqx.broker.support.StressTestSupport - broker stress progress: threads=200, published=74698566, acked=70890784, dispatchReceived=70896160, intervalThroughput=120882 msg/s, elapsed=580.0s
23:19:51.671 [pool-7-thread-1] INFO plus.jmqx.broker.support.StressTestSupport - broker stress progress: threads=200, published=75302586, acked=71509411, dispatchReceived=71509891, intervalThroughput=123654 msg/s, elapsed=585.0s
23:19:56.668 [pool-7-thread-1] INFO plus.jmqx.broker.support.StressTestSupport - broker stress progress: threads=200, published=75929092, acked=72117302, dispatchReceived=72118893, intervalThroughput=121661 msg/s, elapsed=590.0s
23:20:01.670 [pool-7-thread-1] INFO plus.jmqx.broker.support.StressTestSupport - broker stress progress: threads=200, published=76526968, acked=72729026, dispatchReceived=72729478, intervalThroughput=122311 msg/s, elapsed=595.0s
23:20:06.566 [pool-7-thread-1] INFO plus.jmqx.broker.support.StressTestSupport - broker stress progress: threads=200, published=77171000, acked=73338254, dispatchReceived=73338656, intervalThroughput=121866 msg/s, elapsed=600.0s
23:20:11.565 [pool-7-thread-1] INFO plus.jmqx.broker.support.StressTestSupport - broker stress progress: threads=200, published=77315292, acked=73948782, dispatchReceived=73949079, intervalThroughput=122028 msg/s, elapsed=605.0s
23:20:16.568 [pool-7-thread-1] INFO plus.jmqx.broker.support.StressTestSupport - broker stress progress: threads=200, published=77315311, acked=74558271, dispatchReceived=74558752, intervalThroughput=121926 msg/s, elapsed=610.0s
23:20:21.564 [pool-7-thread-1] INFO plus.jmqx.broker.support.StressTestSupport - broker stress progress: threads=200, published=77315316, acked=75163976, dispatchReceived=75164019, intervalThroughput=121130 msg/s, elapsed=615.0s
23:20:26.564 [pool-7-thread-1] INFO plus.jmqx.broker.support.StressTestSupport - broker stress progress: threads=200, published=77315317, acked=75774907, dispatchReceived=75774990, intervalThroughput=122174 msg/s, elapsed=620.0s
23:20:31.562 [pool-7-thread-1] INFO plus.jmqx.broker.support.StressTestSupport - broker stress progress: threads=200, published=77315318, acked=76383066, dispatchReceived=76383843, intervalThroughput=121692 msg/s, elapsed=625.0s
23:20:36.559 [pool-7-thread-1] INFO plus.jmqx.broker.support.StressTestSupport - broker stress progress: threads=200, published=77315318, acked=76983958, dispatchReceived=76984485, intervalThroughput=120230 msg/s, elapsed=630.0s
23:20:40.482 [main] INFO plus.jmqx.broker.support.StressTestSupport - broker stress result: threads=200, durationSeconds=600, payloadBytes=64, acked=77315319, dispatchReceived=77315319, time=633.912s, throughput=121965 msg/s, completed=true
```

**集群消息吞吐**（双节点，MQTT 端口分别为解析后的 `port` 与 `port + 1000`，集群通信端口默认 7771/7772，启动前同样会做可用端口解析）

```shell
MAVEN_OPTS="-Xmx4g" mvn test -pl jmqx-cluster \
  -Djmqx.integration.tests=true \
  -Dtest=ClusterStressTest \
  -Djmqx.stress.durationSeconds=600 \
  -Djmqx.stress.threads=200 \
  -Djmqx.stress.reportIntervalSeconds=5
```

**集群压测补充说明**

- 客户端均分到两个节点（各 `threads / 2`），先逐批建连再并发发布；客户端使用共享连接池、`cleanSession` 与 keepalive，与 `MassiveConnectionTest` 的连接策略一致。
- 日志中 `published` 通常先于 `acked` 增长，汇总 `throughput` 按整段耗时（含建连、发布、等待 PUBACK 收尾）计算；客户端较多时，前期 `intervalThroughput` 偏低、后期才接近稳态，对比吞吐建议参考进度日志后段的区间值。
- 高并发场景可适当调低 `jmqx.stress.inFlightLimit`（如 `2000`）以缩短收尾等待，或增大 `jmqx.stress.timeoutSeconds`；本机资源有限时，`threads=200` 的汇总吞吐可能低于少量客户端压测，属正常现象。

运行中周期性输出进度，结束时汇总吞吐，例如：

```shell
22:50:21.418 [main] INFO plus.jmqx.broker.support.ClusterStressTestSupport - cluster stress: cluster formed, preflight start
22:50:21.564 [main] INFO plus.jmqx.broker.support.ClusterStressTestSupport - cluster stress: 200/200 clients connected
22:50:26.502 [pool-9-thread-1] INFO plus.jmqx.broker.support.StressTestSupport - cluster stress progress: threads=200, published=4021098, acked=21099, dispatchReceived=34376, intervalThroughput=4151 msg/s, elapsed=5.1s
22:50:31.426 [pool-9-thread-1] INFO plus.jmqx.broker.support.StressTestSupport - cluster stress progress: threads=200, published=4050803, acked=54214, dispatchReceived=88392, intervalThroughput=6723 msg/s, elapsed=10.0s
22:50:36.424 [pool-9-thread-1] INFO plus.jmqx.broker.support.StressTestSupport - cluster stress progress: threads=200, published=4083299, acked=94616, dispatchReceived=252368, intervalThroughput=8084 msg/s, elapsed=15.0s
22:50:41.552 [pool-9-thread-1] INFO plus.jmqx.broker.support.StressTestSupport - cluster stress progress: threads=200, published=4179597, acked=209869, dispatchReceived=337719, intervalThroughput=23040 msg/s, elapsed=20.0s
22:50:46.427 [pool-9-thread-1] INFO plus.jmqx.broker.support.StressTestSupport - cluster stress progress: threads=200, published=4344656, acked=352774, dispatchReceived=403143, intervalThroughput=28576 msg/s, elapsed=25.0s
...
22:55:51.425 [pool-9-thread-1] INFO plus.jmqx.broker.support.StressTestSupport - cluster stress progress: threads=200, published=32950221, acked=28950350, dispatchReceived=28956663, intervalThroughput=91707 msg/s, elapsed=330.0s
22:55:56.425 [pool-9-thread-1] INFO plus.jmqx.broker.support.StressTestSupport - cluster stress progress: threads=200, published=33492692, acked=29492713, dispatchReceived=29493106, intervalThroughput=108475 msg/s, elapsed=335.0s
22:56:01.422 [pool-9-thread-1] INFO plus.jmqx.broker.support.StressTestSupport - cluster stress progress: threads=200, published=33985945, acked=29986159, dispatchReceived=29992648, intervalThroughput=98736 msg/s, elapsed=340.0s
22:56:06.431 [pool-9-thread-1] INFO plus.jmqx.broker.support.StressTestSupport - cluster stress progress: threads=200, published=34384157, acked=30385263, dispatchReceived=30397882, intervalThroughput=79759 msg/s, elapsed=345.0s
22:56:11.425 [pool-9-thread-1] INFO plus.jmqx.broker.support.StressTestSupport - cluster stress progress: threads=200, published=34896376, acked=30896377, dispatchReceived=30897062, intervalThroughput=102257 msg/s, elapsed=350.0s
...
23:00:01.424 [pool-9-thread-1] INFO plus.jmqx.broker.support.StressTestSupport - cluster stress progress: threads=200, published=56905905, acked=52905941, dispatchReceived=52905955, intervalThroughput=107804 msg/s, elapsed=580.0s
23:00:06.419 [pool-9-thread-1] INFO plus.jmqx.broker.support.StressTestSupport - cluster stress progress: threads=200, published=57391998, acked=53392203, dispatchReceived=53399152, intervalThroughput=97335 msg/s, elapsed=585.0s
23:00:11.421 [pool-9-thread-1] INFO plus.jmqx.broker.support.StressTestSupport - cluster stress progress: threads=200, published=57857105, acked=53857106, dispatchReceived=53862080, intervalThroughput=92967 msg/s, elapsed=590.0s
23:00:16.426 [pool-9-thread-1] INFO plus.jmqx.broker.support.StressTestSupport - cluster stress progress: threads=200, published=58380810, acked=54380811, dispatchReceived=54381563, intervalThroughput=104666 msg/s, elapsed=595.0s
23:00:21.424 [pool-9-thread-1] INFO plus.jmqx.broker.support.StressTestSupport - cluster stress progress: threads=200, published=58877043, acked=54877108, dispatchReceived=54878850, intervalThroughput=99270 msg/s, elapsed=600.0s
23:00:26.419 [pool-9-thread-1] INFO plus.jmqx.broker.support.StressTestSupport - cluster stress progress: threads=200, published=58924045, acked=55670215, dispatchReceived=55772168, intervalThroughput=158759 msg/s, elapsed=605.0s
23:00:31.422 [pool-9-thread-1] INFO plus.jmqx.broker.support.StressTestSupport - cluster stress progress: threads=200, published=58924099, acked=56799078, dispatchReceived=56799165, intervalThroughput=225630 msg/s, elapsed=610.0s
23:00:36.433 [pool-9-thread-1] INFO plus.jmqx.broker.support.StressTestSupport - cluster stress progress: threads=200, published=58924106, acked=57674632, dispatchReceived=57697504, intervalThroughput=175093 msg/s, elapsed=615.0s
23:00:41.424 [pool-9-thread-1] INFO plus.jmqx.broker.support.StressTestSupport - cluster stress progress: threads=200, published=58924109, acked=58652701, dispatchReceived=58666181, intervalThroughput=195569 msg/s, elapsed=620.0s
23:00:44.064 [main] INFO plus.jmqx.broker.support.StressTestSupport - cluster stress result: threads=200, durationSeconds=600, payloadBytes=64, acked=58924110, dispatchReceived=58924110, time=622.581s, throughput=94645 msg/s, completed=true
```

> 压测公共代码位于各模块 `src/test/java/plus/jmqx/broker/support/`（`StressTestSupport`、`MqttStressClient`、`MqttKeepaliveClient` 等）。broker 与 cluster 模块各自维护一份，互不依赖 test-jar。

- 单元测试方式启动：本机集群

引入依赖

```xml
        <dependency>
            <groupId>plus.jmqx.iot</groupId>
            <artifactId>jmqx-cluster</artifactId>
            <version>1.4.12</version>
        </dependency>
```

编写测试用例

```java
/**
 * 集群测试用例
 */
@Slf4j
public class BootstrapTest {
    @Test
    void cluster01() throws Exception {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        loggerContext.getLogger("root").setLevel(Level.INFO);
        loggerContext.getLogger("plus.jmqx.broker").setLevel(Level.INFO);
        loggerContext.getLogger("plus.jmqx.broker.cluster").setLevel(Level.DEBUG);
        loggerContext.getLogger("reactor.netty").setLevel(Level.INFO);
        MqttConfiguration config = new MqttConfiguration();
        config.getClusterConfig().setEnable(true);
        config.getClusterConfig().setUrl("127.0.0.1:7771,127.0.0.1:7772");
        config.getClusterConfig().setPort(7771);
        config.getClusterConfig().setNode("node-1");
        config.getClusterConfig().setNamespace("jmqx");
        Bootstrap bootstrap = new Bootstrap(config);
        bootstrap.start().block();
        Thread.sleep(3600 * 1000);
        bootstrap.shutdown();
    }

    @Test
    void cluster02() throws Exception {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        loggerContext.getLogger("root").setLevel(Level.INFO);
        loggerContext.getLogger("plus.jmqx.broker").setLevel(Level.INFO);
        loggerContext.getLogger("plus.jmqx.broker.cluster").setLevel(Level.DEBUG);
        loggerContext.getLogger("reactor.netty").setLevel(Level.INFO);
        MqttConfiguration config = new MqttConfiguration();
        config.setPort(2883);
        config.setSecurePort(2884);
        config.setWebsocketPort(9883);
        config.setWebsocketSecurePort(9884);
        config.getClusterConfig().setEnable(true);
        config.getClusterConfig().setUrl("127.0.0.1:7771,127.0.0.1:7772");
        config.getClusterConfig().setPort(7772);
        config.getClusterConfig().setNode("node-2");
        config.getClusterConfig().setNamespace("jmqx");
        Bootstrap bootstrap = new Bootstrap(config);
        bootstrap.start().block();
        Thread.sleep(3600 * 1000);
        bootstrap.shutdown();
    }
}
```

启动测试用例控制台输出

```shell
14:21:52.411 [main] INFO io.scalecube.cluster.Cluster - [null][doStart] Starting, config: ClusterConfig[metadata=null, metadataTimeout=3000, metadataCodec=io.scalecube.cluster.metadata.JdkMetadataCodec@57ad2aa7, memberId='null', memberAlias='node-1', externalHost='null', externalPort=null, transportConfig=TransportConfig[port=7771, clientSecured=false, connectTimeout=3000, messageCodec=plus.jmqx.broker.cluster.JacksonMessageCodec@5b3f61ff, maxFrameLength=2097152, transportFactory=io.scalecube.transport.netty.tcp.TcpTransportFactory@3e2059ae, addressMapper=java.util.function.Function$$Lambda$1/1560911714@398dada8], failureDetectorConfig=FailureDetectorConfig[pingInterval=1000, pingTimeout=500, pingReqMembers=3], gossipConfig=GossipConfig[gossipFanout=3, gossipInterval=200, gossipRepeatMult=3, gossipSegmentationThreshold=1000], membershipConfig=MembershipConfig[seedMembers=[127.0.0.1:7771, 127.0.0.1:7772], syncInterval=30000, syncTimeout=3000, suspicionMult=5, namespace='jmqx', removedMembersHistorySize=42]]
14:21:52.518 [sc-cluster-io-nio-1] INFO io.scalecube.cluster.transport.api.Transport - [start][/0:0:0:0:0:0:0:0:7771] Bound cluster transport
14:21:52.536 [sc-cluster-io-nio-1] WARN io.scalecube.cluster.membership.MembershipProtocol - [jmqx:node-1:c2bb3af63614e2a@127.0.0.1:7771] Filtering out seed address: 127.0.0.1:7771
14:21:52.540 [sc-cluster-io-nio-1] INFO io.scalecube.cluster.membership.MembershipProtocol - [jmqx:node-1:c2bb3af63614e2a@127.0.0.1:7771] Making initial Sync to all seed members: [127.0.0.1:7772]
14:21:52.619 [sc-cluster-io-nio-1] WARN io.scalecube.cluster.transport.api.Transport - [127.0.0.1:7771][connect][error] remoteAddress: 127.0.0.1:7772, cause: io.netty.channel.AbstractChannel$AnnotatedConnectException: Connection refused: /127.0.0.1:7772
14:21:52.619 [sc-cluster-io-nio-1] WARN io.scalecube.cluster.membership.MembershipProtocol - [jmqx:node-1:c2bb3af63614e2a@127.0.0.1:7771] Exception on initial Sync, cause: io.netty.channel.AbstractChannel$AnnotatedConnectException: Connection refused: /127.0.0.1:7772
14:21:52.620 [sc-cluster-7771-1] INFO io.scalecube.cluster.Cluster - [jmqx:node-1:c2bb3af63614e2a@127.0.0.1:7771][doStart] Started
14:21:52.625 [jmqx-event-loop-select-nio-2] INFO plus.jmqx.broker.mqtt.transport.impl.MqttTransport - mqtt broker start success host 0:0:0:0:0:0:0:0 port 1883
14:21:52.626 [jmqx-event-loop-select-nio-3] INFO plus.jmqx.broker.mqtt.transport.impl.MqttTransport - mqtt-ws broker start success host 0:0:0:0:0:0:0:0 port 1884
```

```shell
14:22:39.104 [main] INFO io.scalecube.cluster.Cluster - [null][doStart] Starting, config: ClusterConfig[metadata=null, metadataTimeout=3000, metadataCodec=io.scalecube.cluster.metadata.JdkMetadataCodec@57ad2aa7, memberId='null', memberAlias='node-2', externalHost='null', externalPort=null, transportConfig=TransportConfig[port=7772, clientSecured=false, connectTimeout=3000, messageCodec=plus.jmqx.broker.cluster.JacksonMessageCodec@5b3f61ff, maxFrameLength=2097152, transportFactory=io.scalecube.transport.netty.tcp.TcpTransportFactory@3e2059ae, addressMapper=java.util.function.Function$$Lambda$1/1560911714@398dada8], failureDetectorConfig=FailureDetectorConfig[pingInterval=1000, pingTimeout=500, pingReqMembers=3], gossipConfig=GossipConfig[gossipFanout=3, gossipInterval=200, gossipRepeatMult=3, gossipSegmentationThreshold=1000], membershipConfig=MembershipConfig[seedMembers=[127.0.0.1:7771, 127.0.0.1:7772], syncInterval=30000, syncTimeout=3000, suspicionMult=5, namespace='jmqx', removedMembersHistorySize=42]]
14:22:39.212 [sc-cluster-io-nio-1] INFO io.scalecube.cluster.transport.api.Transport - [start][/0:0:0:0:0:0:0:0:7772] Bound cluster transport
14:22:39.228 [sc-cluster-io-nio-1] WARN io.scalecube.cluster.membership.MembershipProtocol - [jmqx:node-2:259b35b190434e83@127.0.0.1:7772] Filtering out seed address: 127.0.0.1:7772
14:22:39.232 [sc-cluster-io-nio-1] INFO io.scalecube.cluster.membership.MembershipProtocol - [jmqx:node-2:259b35b190434e83@127.0.0.1:7772] Making initial Sync to all seed members: [127.0.0.1:7771]
14:22:39.511 [sc-cluster-7772-1] INFO io.scalecube.cluster.membership.MembershipProtocol - [jmqx:node-2:259b35b190434e83@127.0.0.1:7772][publishEvent] MembershipEvent[type=ADDED, member=jmqx:node-1:c2bb3af63614e2a@127.0.0.1:7771, oldMetadata=null, newMetadata=7e16449-5, timestamp=2025-04-23T06:22:39.510Z]
14:22:39.514 [sc-cluster-7772-1] INFO plus.jmqx.broker.cluster.ScubeClusterRegistry - cluster onMembershipEvent jmqx:node-1:c2bb3af63614e2a@127.0.0.1:7771  MembershipEvent[type=ADDED, member=jmqx:node-1:c2bb3af63614e2a@127.0.0.1:7771, oldMetadata=null, newMetadata=7e16449-5, timestamp=2025-04-23T06:22:39.510Z]
14:22:39.515 [sc-cluster-7772-1] INFO io.scalecube.cluster.Cluster - [jmqx:node-2:259b35b190434e83@127.0.0.1:7772][doStart] Started
14:22:39.518 [jmqx-event-loop-select-nio-2] INFO plus.jmqx.broker.mqtt.transport.impl.MqttTransport - mqtt broker start success host 0:0:0:0:0:0:0:0 port 2883
14:22:39.519 [jmqx-event-loop-select-nio-3] INFO plus.jmqx.broker.mqtt.transport.impl.MqttTransport - mqtt-ws broker start success host 0:0:0:0:0:0:0:0 port 2884
```

## 生成证书

> 这里采用[generate-CA.sh](https://github.com/owntracks/tools/blob/master/TLS/generate-CA.sh)来生成双向认证自签证书

从 https://github.com/owntracks/tools/blob/master/TLS/generate-CA.sh 下载证书生成脚本

```shell
# 生成服务端证书
./generate-CA.sh server
# 生成客户端证书
./generate-CA.sh client client
```
