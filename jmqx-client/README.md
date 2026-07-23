# jmqx-client — Reactor-Netty MQTT Client

基于 reactor-netty + netty-codec-mqtt 的高性能异步 MQTT
客户端，对标 [HiveMQ MQTT Client](https://github.com/hivemq/hivemq-mqtt-client) API 风格。

## 特性

- **MQTT 3.1.1 + 5.0** 双协议支持
- **三种 API**：Reactor (`Mono`/`Flux`)、`CompletableFuture`、阻塞
- **双向背压**：入站 `request(n)` 门控 ACK；出站 inflight 信号量
- **自动重连**：指数退避 + jitter（`Mono.delay`，无阻塞 sleep）
- **断线缓存**：离线期间缓冲 QoS1/2 出站消息，重连后自动 flush
- **传输**：TCP / TLS / WebSocket / WSS

## 快速开始（MQTT 3.1.1）

```xml
<dependency>
    <groupId>plus.jmqx.iot</groupId>
    <artifactId>jmqx-client</artifactId>
    <version>1.4.18</version>
</dependency>
```

```java
// @formatter:off
import plus.jmqx.client.mqtt.MqttClient;
import plus.jmqx.client.mqtt.message.QoS;
import plus.jmqx.client.mqtt.v3.Mqtt3RxClient;
import plus.jmqx.client.mqtt.v3.message.Mqtt3Publish;
import plus.jmqx.client.mqtt.v3.message.Mqtt3Subscribe;

import java.time.Duration;

Mqtt3RxClient client = MqttClient.builder().useMqttVersion3()
        .serverHost("localhost").serverPort(1883)
        .identifier("my-client")
        .automaticReconnect()
        .buildRx();

client.connect().block(Duration.ofSeconds(5));

client.subscribePublishes(Mqtt3Subscribe.builder()
        .topicFilters(List.of(Mqtt3TopicFilter.builder().topicFilter("sensor/#").qos(QoS.AT_LEAST_ONCE).build()))
        .build())
        .doOnNext(p ->System.out.println(p.getTopic() +": "+new String(p.getPayloadAsBytes())))
        .subscribe();

client.publish(Mqtt3Publish.builder()
        .topic("sensor/temp").payload("21.5".getBytes()).qos(QoS.AT_LEAST_ONCE).build())
        .block();
// @formatter:on
```

## 快速开始（MQTT 5.0）

```java
// @formatter:off
Mqtt5RxClient client = MqttClient.builder().useMqttVersion5()
        .serverHost("localhost").serverPort(1883)
        .identifier("v5-client")
        .cleanStart(true)
        .sessionExpiryInterval(3600)
        .receiveMaximum(100)
        .automaticReconnect()
        .buildRx();

client.connect().block(Duration.ofSeconds(5));
// @formatter:on
```

## API 视图

| 协议 | Reactor         | Async              | Blocking              |
|----|-----------------|--------------------|-----------------------|
| v3 | `Mqtt3RxClient` | `Mqtt3AsyncClient` | `Mqtt3BlockingClient` |
| v5 | `Mqtt5RxClient` | `Mqtt5AsyncClient` | `Mqtt5BlockingClient` |

构建方式：

```java
// @formatter:off
MqttClient.builder().useMqttVersion3()  // 或 useMqttVersion5()
    .serverHost("localhost").serverPort(1883)
    .identifier("id")
    .buildRx();    // 或 buildAsync() / buildBlocking()
// @formatter:on
```

## 测试

测试分为三层：**单元测试**（默认 `mvn test`）、**集成测试**（`*IT.java`）、**压力测试**（`*StressTest.java`）。后两者被 surefire 默认排除，需显式指定 `-Dtest=...` 运行。

### 前置条件

集成测试依赖 `jmqx-broker`（test scope，用于内嵌 broker）。首次运行前需安装 broker 模块：

```bash
mvn -pl jmqx-broker install -DskipTests
```

压测**不**使用内嵌 broker，无需上述依赖即可编译，但运行前须自行准备好外部 MQTT broker。

### 单元测试

覆盖编解码、背压、ACK 跟踪、断线缓存、自动重连、主题匹配等核心逻辑，**不依赖外部 broker**：

| 测试类 | 覆盖范围 |
|--------|----------|
| `Mqtt3MessageServiceTest` / `Mqtt5MessageServiceTest` | PUBLISH/SUBSCRIBE 编解码往返 |
| `MqttInboxBackpressureTest` / `MqttOutboxBackpressureTest` | 入站/出站背压与 inflight 控制 |
| `AckTrackerTest` | QoS1/2 出站 ACK 跟踪 |
| `MessageBufferTest` | 断线期间出站消息缓冲 |
| `MqttAutoReconnectTest` | 自动重连退避策略 |
| `TopicMatcherTest` / `PacketIdManagerTest` | 主题通配符匹配、packetId 分配 |
| `QoSTest` | QoS 枚举与转换 |

```bash
# 默认运行（排除 *IT.java 与 *StressTest.java）
mvn -pl jmqx-client test
```

### 集成测试

`Mqtt3ClientIT`（15 个用例）与 `Mqtt5ClientIT`（12 个用例）验证 **jmqx-client ↔ jmqx-broker** 端到端行为。

**Broker 模式：**

- **默认**：JVM 内自动启动内嵌 jmqx-broker（TCP + MQTTS + WS + WSS，随机端口），无需手动部署
- **外部 broker**：`-Djmqx.it.broker.port=1883`（可选 `-Djmqx.it.broker.host=...`）
- **传输层端口**（对接外部 broker 时，默认与 jmqx-broker 一致）：TCP `1883`、MQTTS `8883`、WS `1884`、WSS `8884`，可通过 `-Djmqx.it.broker.port` / `securePort` / `websocketPort` / `websocketSecurePort` 覆盖

**MQTT 3.1.1 覆盖（`Mqtt3ClientIT`）：**

| 场景 | 说明 |
|------|------|
| 连接/断开 | CONNACK 接受、正常 DISCONNECT |
| QoS 0/1/2 发布订阅 | 参数化覆盖全部 QoS 级别 |
| 三种 API | Reactor（Rx）、`CompletableFuture`（Async）、Blocking |
| 通配符订阅 | `#` 与 `+` 主题过滤 |
| 取消订阅 | UNSUBSCRIBE 后不再投递 |
| 双客户端 | 独立 publisher / subscriber |
| Retain 消息 | 晚加入订阅者仍能收到 retain |
| 会话持久化 | cleanSession=false 时离线消息重连后投递 |
| 多 topic 订阅 | 单次 SUBSCRIBE 多个 filter |
| 空 payload / 并发发布 | 边界与 Async 并发场景 |

**MQTT 5.0 覆盖（`Mqtt5ClientIT`）：**

| 场景 | 说明 |
|------|------|
| 连接/断开、QoS 0/1/2 | 同 v3 基础链路 |
| User Properties / Content-Type | v5 发布属性 |
| 三种 API、通配符、取消订阅 | 同 v3 |
| 双客户端、Retain | 同 v3 |
| CONNACK receiveMaximum | v5 会话协商属性 |

**传输层覆盖（`Mqtt3TransportIT` / `Mqtt5TransportIT`，各 3 个用例）：**

| 传输 | 默认端口 | 说明 |
|------|----------|------|
| TCP | 1883 | 明文 MQTT（`Mqtt3ClientIT` / `Mqtt5ClientIT`） |
| MQTTS (TLS) | 8883 | 连接 + QoS1 发布订阅 |
| WS | 1884 | WebSocket `/mqtt` 子协议 |
| WSS | 8884 | WebSocket over TLS |

```bash
# 运行全部集成测试（33 个用例，内嵌 broker）
mvn -pl jmqx-client test -Dtest='Mqtt3ClientIT,Mqtt5ClientIT,Mqtt3TransportIT,Mqtt5TransportIT'

# 单独运行
mvn -pl jmqx-client test -Dtest=Mqtt3ClientIT
mvn -pl jmqx-client test -Dtest=Mqtt5ClientIT
mvn -pl jmqx-client test -Dtest=Mqtt3TransportIT
mvn -pl jmqx-client test -Dtest=Mqtt5TransportIT

# 对接本机已有 broker（如 jmqx-broker 或 EMQX）
mvn -pl jmqx-client test -Dtest='Mqtt3ClientIT,Mqtt5ClientIT,Mqtt3TransportIT,Mqtt5TransportIT' \
  -Djmqx.it.broker.port=1883 \
  -Djmqx.it.broker.securePort=8883 \
  -Djmqx.it.broker.websocketPort=1884 \
  -Djmqx.it.broker.websocketSecurePort=8884
```

### 压力测试

`Mqtt3ClientStressTest` 与 `Mqtt5ClientStressTest` 各含 **3 个独立场景**，分别压测连接、发布、订阅，互不混合。**不启动内嵌 broker**，须自行准备好 MQTT broker 后再运行。须设置 `-Djmqx.stress.tests=true` 才会执行。

**三类场景：**

| 方法 | 场景 | 主要参数 | 度量 |
|------|------|----------|------|
| `connectStress` | 连接压测 | `connections`、`threads`、`connectionHoldSeconds` | 同时在线连接数（peakActive） |
| `publishStress` | 发布压测 | `messages`、`qos`、`threads`、`inflight` | 出站 msg/s（**每条等 broker ACK**） |
| `subscribeStress` | 订阅压测 | `messages`、`qos`、`subscribers`、`publishers` | 入站接收 msg/s |

**前置：启动 broker**

压测不会自动启动 broker。请自行部署并确保对应传输端口可连接（默认与 jmqx-broker 一致）：

| 传输 | 默认端口 | 属性 `jmqx.client.stress.transport` |
|------|----------|-------------------------------------|
| TCP | 1883 | `tcp`（默认） |
| MQTTS | 8883 | `mqtts` |
| WS | 1884 | `ws` |
| WSS | 8884 | `wss` |

例如：

- 在应用中内嵌 `jmqx-broker`（参考 `EmbeddedBrokerHolder`，须开启四监听）
- 使用 EMQX、Mosquitto 等独立 broker
- Docker：`docker run -d --name emqx -p 1883:1883 -p 8883:8883 -p 8083:8083 -p 8084:8084 emqx/emqx`

**运行示例：**

```bash
# 连接压测（500 连接，保持 60s 同时在线）
mvn -P osx-aarch-64 -pl jmqx-client test -Djmqx.stress.tests=true \
  -Dtest=Mqtt3ClientStressTest#connectStress \
  -Djmqx.client.stress.connections=500 \
  -Djmqx.client.stress.threads=32 \
  -Djmqx.client.stress.connectionHoldSeconds=60 \
  -Djmqx.client.stress.timeoutSeconds=300

# 发布压测（QoS1 须等 PUBACK；大批量请加大 timeout）
mvn -P osx-aarch-64 -pl jmqx-client test -Djmqx.stress.tests=true \
  -Dtest=Mqtt3ClientStressTest#publishStress \
  -Djmqx.client.stress.messages=10000000 \
  -Djmqx.client.stress.threads=8 \
  -Djmqx.client.stress.qos=1 \
  -Djmqx.client.stress.inflight=512 \
  -Djmqx.client.stress.timeoutSeconds=3600 \
  -Djmqx.client.stress.minThroughput=0

# 订阅压测
mvn -P osx-aarch-64 -pl jmqx-client test -Djmqx.stress.tests=true \
  -Dtest=Mqtt3ClientStressTest#subscribeStress \
  -Djmqx.client.stress.messages=100000 \
  -Djmqx.client.stress.qos=1

# MQTTS 发布压测（默认连 8883，自签证书用 insecureTrustAll）
mvn -pl jmqx-client test -Djmqx.stress.tests=true \
  -Dtest=Mqtt3ClientStressTest#publishStress \
  -Djmqx.client.stress.transport=mqtts \
  -Djmqx.client.stress.messages=10000 -Djmqx.client.stress.qos=1

# WebSocket 连接压测
mvn -pl jmqx-client test -Djmqx.stress.tests=true \
  -Dtest=Mqtt5ClientStressTest#connectStress \
  -Djmqx.client.stress.transport=ws \
  -Djmqx.client.stress.connections=100

# 带用户名密码（broker 开启鉴权时）
mvn -pl jmqx-client test -Djmqx.stress.tests=true \
  -Dtest=Mqtt3ClientStressTest#publishStress \
  -Djmqx.client.stress.broker.username=stress \
  -Djmqx.client.stress.broker.password=secret \
  -Djmqx.client.stress.messages=1000

# 按场景过滤整类运行（不指定 #方法名时）
mvn -pl jmqx-client test -Djmqx.stress.tests=true \
  -Dtest=Mqtt3ClientStressTest \
  -Djmqx.client.stress.scenario=publish
```

**可调参数**（系统属性 `-Djmqx.client.stress.*`）：

| 属性 | 默认值 | 说明 |
|------|--------|------|
| `jmqx.client.stress.scenario` | `all` | 场景过滤：`connect` / `publish` / `subscribe` / `all` |
| `jmqx.client.stress.transport` | `tcp` | 传输层：`tcp` / `mqtts` / `ws` / `wss` |
| `jmqx.client.stress.messages` | 2000 | 发布/订阅压测消息数 |
| `jmqx.client.stress.threads` | 4 | 连接/发布并发线程数 |
| `jmqx.client.stress.publishers` | 1 | 订阅压测中的灌流发布端数量 |
| `jmqx.client.stress.subscribers` | 1 | 订阅压测中的订阅客户端数 |
| `jmqx.client.stress.connections` | 50 | 连接压测目标连接数 |
| `jmqx.client.stress.connectionHoldSeconds` | 30 | 全部建连后保持时长（秒），用于压同时在线连接 |
| `jmqx.client.stress.payloadBytes` | 64 | 单条 payload 字节数 |
| `jmqx.client.stress.qos` | 0 | QoS 级别（0/1/2） |
| `jmqx.client.stress.inflight` | 256 | 发布侧滑动窗口（同时在途未 ACK 条数上限） |
| `jmqx.client.stress.minThroughput` | 100 | 最低吞吐阈值（msg/s 或 conn/s 场景自适应） |
| `jmqx.client.stress.timeoutSeconds` | `120`（≤5 万条）/ 按消息量自动估算 | 超时秒数；未显式设置且 `messages` > 50000 时按约 5 万 msg/s 保守估算 |
| `jmqx.client.stress.topic` | `stress/client/topic` | 测试 topic 前缀 |
| `jmqx.client.stress.broker.host` | `localhost` | broker 地址 |
| `jmqx.client.stress.broker.port` | `1883` | TCP 端口（`transport=tcp`） |
| `jmqx.client.stress.broker.securePort` | `8883` | MQTTS 端口 |
| `jmqx.client.stress.broker.websocketPort` | `1884` | WS 端口 |
| `jmqx.client.stress.broker.websocketSecurePort` | `8884` | WSS 端口 |
| `jmqx.client.stress.broker.username` | — | MQTT 用户名（未设置则匿名） |
| `jmqx.client.stress.broker.password` | — | MQTT 密码 |
| `jmqx.client.stress.progressIntervalSeconds` | 5 | 进度日志间隔（秒），设为 0 关闭 |
| `jmqx.client.stress.logLevel` | WARN | 压测日志级别 |

### Netty 内存泄漏检测（可选）

排查 ByteBuf 引用计数问题时，可开启 Netty 泄漏检测：

```bash
mvn -pl jmqx-client test -Dtest='Mqtt3ClientIT,Mqtt5ClientIT' \
  -Dio.netty.leakDetection.level=paranoid
```

入站 PUBLISH 的 payload 由 `MqttDecoder` 以 retained slice 分配，客户端在 `InboundQos` 消费后负责 `release()`。

## 架构

```
API 层 (Mqtt3/5 Async/Rx/Blocking)
    ↓
协议适配层 (Mqtt3MessageService / Mqtt5MessageService)
    ↓
核心引擎 (MqttClientEngine: Inbox/Outbox/AckTracker/SubscriptionStore/MessageBuffer)
    ↓
传输层 (TcpClient/HttpClient + MqttEncoder/MqttDecoder pipeline)
```

设计文档：`docs/superpowers/specs/2026-07-02-jmqx-client-design.md`
