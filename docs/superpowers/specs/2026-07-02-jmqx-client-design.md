# jmqx-client MQTT Client 设计文档

- **日期**: 2026-07-02
- **状态**: 设计阶段
- **对标**: HiveMQ MQTT Client

## 1. 概述

jmqx-client 是一个基于 Netty NIO 的高性能异步 MQTT 客户端库，对标 HiveMQ MQTT Client。从第一版开始就按 MQTT 版本区分 `v3` 和 `v5` 子包，确保后续扩展 MQTT 5 时无需破坏性重构。提供 CompletableFuture、Reactor、Blocking 三种 API 视图，内置自动重连、背压、断线缓存等企业级特性。

### 设计目标

- **高性能**：基于 Netty NIO + Reactor Netty，零拷贝编解码
- **三面 API**：一套核心引擎，三种 API 视图（Future / Reactor / Blocking）
- **对等 HiveMQ**：功能特性、API 设计、稳定性对标 HiveMQ MQTT Client
- **轻量嵌入**：无 DI 框架依赖，纯 Java SPI + 构造器注入
- **与 jmqx-broker 风格一致**：使用 Reactor（非 RxJava）、Lombok、SLF4J

## 2. 架构

### 2.1 分层架构

```
┌──────────────────────────────────────────────────────┐
│  API 层                                              │
│  ┌────────────────┐ ┌──────────────┐ ┌────────────┐ │
│  │ MqttAsyncClient│ │ MqttRxClient │ │MqttBlocking│ │
│  │ (CFuture)      │ │ (Reactor)    │ │ (Sync)     │ │
│  └───────┬────────┘ └──────┬───────┘ └─────┬──────┘ │
│          │                 │               │        │
│          └─────────────────┼───────────────┘        │
│                            ▼                        │
│                   ┌────────────────┐                │
│                   │  DefaultMqttClient (Core)       │
│                   │  Reactor-based engine           │
│                   └────────────────┘                │
├──────────────────────────────────────────────────────┤
│  引擎层 (Reactor Netty + Netty Codec MQTT)           │
│  ┌───────────┐ ┌──────────┐ ┌────────────────────┐  │
│  │ Connection│ │ Handler  │ │ ACK Tracker        │  │
│  │ Manager   │ │ Pipeline │ │ (QoS0/1/2)         │  │
│  └───────────┘ └──────────┘ └────────────────────┘  │
│  ┌───────────┐ ┌──────────┐ ┌────────────────────┐  │
│  │ Reconnect │ │ Message  │ │ Subscriptions      │  │
│  │ Handler   │ │ Buffer   │ │ Manager            │  │
│  └───────────┘ └──────────┘ └────────────────────┘  │
├──────────────────────────────────────────────────────┤
│  传输层                                              │
│  ┌───────────────────────────────────────────────┐   │
│  │  Netty NIO (TCP) / Netty WS (WebSocket)       │   │
│  └───────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────┘
```

### 2.2 版本化 API

按 MQTT 版本划分接口，顶部 `MqttClient` 提供版本选择入口：

```java
// === 版本无关的入口 ===
public interface MqttClient<C extends MqttClientConfig> {
    C getConfig();
    MqttClientState getState();
    MqttVersion getVersion();           // MQTT_3_1_1 或 MQTT_5

    static MqttClientBuilder builder();
}

// === MQTT 3.1.1 客户端接口 ===
public interface Mqtt3Client extends MqttClient<Mqtt3ClientConfig> {
    Mqtt3AsyncClient toAsync();
    Mqtt3RxClient toRx();
    Mqtt3BlockingClient toBlock();
}

// === MQTT 5 客户端接口（预留，第一版无实现）===
public interface Mqtt5Client extends MqttClient<Mqtt5ClientConfig> {
    Mqtt5AsyncClient toAsync();
    Mqtt5RxClient toRx();
    Mqtt5BlockingClient toBlock();
}

// === Async API (CompletableFuture) ===
public interface Mqtt3AsyncClient extends Mqtt3Client {
    CompletableFuture<Mqtt3ConnAck> connect();
    CompletableFuture<Mqtt3SubAck> subscribe(Mqtt3Subscribe sub, Consumer<Mqtt3Publish> callback);
    CompletableFuture<Mqtt3PublishResult> publish(Mqtt3Publish publish);
    CompletableFuture<Void> unsubscribe(Mqtt3Unsubscribe unsub);
    CompletableFuture<Void> disconnect();
}

// === Reactive API (Reactor) ===
public interface Mqtt3RxClient extends Mqtt3Client {
    Mono<Mqtt3ConnAck> connect();
    Mono<Mqtt3SubAck> subscribe(Mqtt3Subscribe sub);
    Flux<Mqtt3Publish> subscribePublishes(Mqtt3Subscribe sub);
    Flux<Mqtt3Publish> publishes(MqttGlobalPublishFilter filter);
    Mono<Mqtt3PublishResult> publish(Mqtt3Publish publish);
    Mono<Void> unsubscribe(Mqtt3Unsubscribe unsub);
    Mono<Void> disconnect();
}

// === Blocking API ===
public interface Mqtt3BlockingClient extends Mqtt3Client {
    Mqtt3ConnAck connect();
    Mqtt3SubAck subscribe(Mqtt3Subscribe sub);
    Mqtt3Publishes publishes(MqttGlobalPublishFilter filter);
    void publish(Mqtt3Publish publish);
    void unsubscribe(Mqtt3Unsubscribe unsub);
    void disconnect();
}

### 2.3 Builder API

在构建器入口选择 MQTT 版本：

```java
// 方式一：通过 builder 入口选择版本
Mqtt3Client client = MqttClient.builder()
    .useMqttVersion3()                          // ← 选择 MQTT 3.1.1
        .serverHost("localhost")
        .serverPort(1883)
        .identifier("my-client")
        .automaticReconnect()
            .initialDelay(1, TimeUnit.SECONDS)
            .maxDelay(120, TimeUnit.SECONDS)
            .apply()
        .willPublish()
            .topic("last/will")
            .payload(b"offline")
            .qos(QoS.AT_LEAST_ONCE)
            .apply()
        .addConnectedListener(event -> log.info("connected"))
        .addDisconnectedListener(event -> log.warn("disconnected"))
        .buildAsync();  // 或 buildRx() / buildBlocking()

// 方式二：直接使用 Mqtt3Client.builder()
Mqtt3Client client2 = Mqtt3Client.builder()
    .serverHost("localhost")
    .serverPort(1883)
    .buildRx();
```

## 3. 状态机

```
         ┌──────────────────────────────────────┐
         │                                      │
         ▼                                      │
   ┌──────────┐ connect()  ┌──────────┐         │
   │DISCONNECTED│──────────►│ CONNECTING│        │
   └─────┬─────┘           └─────┬────┘         │
         │                       │               │
         │ 意外断开               │ CONNACK 收到   │
         │                       ▼               │
         │                 ┌──────────┐          │
         │                 │ CONNECTED│          │
         │                 └─────┬────┘          │
         │                       │               │
         │  disconnect()         │               │
         │                       ▼               │
         │                 ┌──────────┐          │
         └─────────────────│DISCONNECTED│─────────┘
                           └──────────┘
```

状态（`MqttClientState` 枚举）：`DISCONNECTED`、`CONNECTING`、`CONNECTED`

状态转换规则：
- `DISCONNECTED → CONNECTING`：调用 `connect()` 时
- `CONNECTING → CONNECTED`：收到 CONNACK 后
- `CONNECTED → DISCONNECTED`：调用 `disconnect()` 或连接意外断开

## 4. Netty Pipeline

### 4.1 初始 Pipeline

```
Channel Pipeline:
  MqttEncoder (Outbound)       ← 消息编码：MQTT → ByteBuf
  MqttConnectHandler            ← 写入 CONNECT，等待 CONNACK
  MqttDisconnectHandler         ← 处理 DISCONNECT 报文
```

### 4.2 CONNACK 后动态添加

```
  MqttSubscriptionHandler       ← 维护订阅映射，匹配入站 PUBLISH
  MqttIncomingQosHandler        ← 入站 QoS 1/2 的 ACK 回复
  MqttOutgoingQosHandler        ← 出站 QoS 1/2 的 ACK 跟踪
```

### 4.3 编解码

利用 `netty-codec-mqtt` 的 `MqttEncoder` / `MqttDecoder` 进行编解码。HiveMQ 有自己的编解码器，我们直接复用 Netty 的成熟实现。

## 5. 连接生命周期

```
1. Builder.buildAsync()
2. client.connect()
3. 创建 Netty Bootstrap
4. Channel Active → MqttConnectHandler 写入 CONNECT
5. 等待 CONNACK
   ├── 成功 → 状态 → CONNECTED
   │         → 添加 SubscriptionHandler / QoSHandler
   │         → 通知 ConnectedListeners
   │         → 刷新断线缓存中的消息
   └── 失败 → 状态 → DISCONNECTED
             → 通知 DisconnectedListeners
             → AutoReconnect 开始指数退避重连

6. client.disconnect()
   → 写入 DISCONNECT
   → 关闭 Channel
   → 状态 → DISCONNECTED

7. 意外断开
   → 状态 → DISCONNECTED
   → 通知 DisconnectedListeners
   → AutoReconnect 指数退避
   → 重连成功 → 重新订阅 → 刷新离线缓存
```

## 6. QoS 流程

### 6.1 出站消息（Client → Broker）

| QoS | 流程 |
|---|---|
| 0 | `PUBLISH` → 直接成功，无需等待 |
| 1 | `PUBLISH` → 等待 `PUBACK` → 完成 Future/Mono |
| 2 | `PUBLISH` → 等待 `PUBREC` → `PUBREL` → 等待 `PUBCOMP` → 完成 |

ACK 跟踪方式：
- 每条消息分配 `packetId`（1～65535 循环使用）
- 用 `Map<Integer, Sinks.One<MqttMessage>>` 存储未完成的请求
- `PUBACK`/`PUBREC`/`PUBCOMP` 到达时查表并 complete Sink

### 6.2 入站消息（Broker → Client）

| QoS | 流程 |
|---|---|
| 0 | 直接回调 subscription handler |
| 1 | `PUBLISH` → 回调 → 回复 `PUBACK` |
| 2 | `PUBLISH` → 回调 → 回复 `PUBREC` → 等待 `PUBREL` → 回复 `PUBCOMP` |

## 7. 自动重连

实现为 `MqttClientDisconnectedListener` 的 SPI 实现：

```java
public class MqttAutoReconnect implements MqttClientDisconnectedListener {
    // 指数退避 + 随机 jitter
    // delay = min(startDelay * 2^attempts, maxDelay)
    // delay += random(-25%, +25%)
    //
    // 默认: startDelay=1s, maxDelay=120s
    // 用户通过配置覆盖
}
```

重连策略（通过 `MqttClientReconnector` 控制）：

```java
reconnector
    .reconnect(true)                              // 是否重连
    .resubscribeIfSessionPresent(false)           // 会话存在时是否重订阅
    .resubscribeIfSessionExpired(true)            // 会话过期时是否重订阅
    .republishIfSessionExpired(false)             // 是否重发离线消息
    .delay(1000, TimeUnit.MILLISECONDS);          // 本次重连延迟
```

## 8. 断线缓存

断线期间的 publish 消息暂存在内存队列中：

```java
class MessageBuffer {
    private final ConcurrentLinkedQueue<MqttPublish> buffer;
    private final int maxSize;        // 最大缓存条数，默认 1000
    private final long maxBytes;      // 最大缓存字节，默认 64MB

    boolean offer(MqttPublish publish);        // 返回 false 表示拒绝
    void flush(MqttOutgoingQosHandler handler); // 重连后按序刷新
    void clear();
    int size();
}
```

- 队列满时 `publish()` 返回失败的 `CompletableFuture` / `Mono.error()`
- 重连成功后，按原始顺序依次写入
- `disconnect()` 主动断开**不触发**重连，但缓存仍保留

## 9. 配置模型

```java
public class MqttClientConfig {
    // 连接
    String serverHost = "localhost";
    int serverPort = 1883;
    String clientId;                    // 自动生成 UUID 前缀
    int keepAliveSeconds = 60;

    // 传输
    int socketConnectTimeoutMs = 10_000;
    int mqttConnectTimeoutMs = 60_000;
    boolean sslEnabled = false;
    MqttSslConfig sslConfig;
    boolean webSocketEnabled = false;
    MqttWebSocketConfig webSocketConfig;

    // 线程
    int nettyThreads;                   // 0 = CPU cores
    int businessThreadSize = 16;        // 回调执行线程池

    // 重连
    MqttAutoReconnectConfig reconnectConfig;

    // 缓冲
    int messageBufferMaxSize = 1000;
    long messageBufferMaxBytes = 64 * 1024 * 1024;

    // 认证
    String username;
    byte[] password;

    // 遗嘱
    MqttPublish willPublish;
}
```

## 10. 消息模型

消息类型按版本分包放置。第一版仅实现 MQTT 3.1.1：

```java
// === 版本无关（plus.jmqx.client.mqtt）===
public enum MqttVersion { MQTT_3_1, MQTT_3_1_1, MQTT_5 }
public enum QoS { AT_MOST_ONCE, AT_LEAST_ONCE, EXACTLY_ONCE }

// === MQTT 3.1.1 消息（plus.jmqx.client.mqtt.v3.message）===

// 发布消息
public class Mqtt3Publish {
    String topic;
    byte[] payload;
    QoS qos;
    boolean retain;
    int packetId;
}

// CONNECT 消息
public class Mqtt3Connect {
    String clientId;
    boolean cleanSession;
    int keepAliveSeconds;
    String username;
    byte[] password;
    Mqtt3Publish willPublish;
}

// 订阅
public class Mqtt3Subscribe {
    List<Mqtt3TopicFilter> topicFilters;
    int packetId;
}

// 主题过滤器
public class Mqtt3TopicFilter {
    String topicFilter;
    QoS qos;
}

// 取消订阅
public class Mqtt3Unsubscribe {
    List<String> topicFilters;
    int packetId;
}

// 确认结果（位于 plus.jmqx.client.mqtt.v3.message.*）

public class Mqtt3ConnAck {
    boolean sessionPresent;
    MqttVersion version();
}

public class Mqtt3SubAck {
    List<QoS> grantedQos;
}

public interface Mqtt3PublishResult {
    Mqtt3Publish getPublish();
    Throwable getError();
}

// === MQTT 5 预留包（plus.jmqx.client.mqtt.v5.message）===
// Mqtt5Connect, Mqtt5Publish, Mqtt5Subscribe 等（第一版仅声明包结构）
```

## 11. 文件结构

```
jmqx-client/src/main/java/plus/jmqx/client/
├── annotations/
│   ├── CheckReturnValue.java
│   └── Immutable.java
├── mqtt/
│   ├── MqttClient.java                # 主入口接口
│   ├── MqttClientBuilder.java         # 构建器（.useMqttVersion3() / .useMqttVersion5()）
│   ├── MqttClientConfig.java          # 版本无关配置基类
│   ├── MqttClientState.java           # 状态枚举
│   ├── MqttVersion.java               # 版本枚举
│   ├── MqttGlobalPublishFilter.java   # 入站过滤器
│   │
│   ├── v3/                            # ===== MQTT 3.1/3.1.1 =====
│   │   ├── Mqtt3Client.java           # MQTT 3.x 客户端接口
│   │   ├── Mqtt3ClientConfig.java     # MQTT 3.x 配置
│   │   ├── Mqtt3ClientBuilder.java    # MQTT 3.x 构建器
│   │   ├── Mqtt3AsyncClient.java      # Future API
│   │   ├── Mqtt3RxClient.java         # Reactor API
│   │   ├── Mqtt3BlockingClient.java   # Blocking API
│   │   ├── Mqtt3PublishResult.java    # 发布结果
│   │   │
│   │   ├── message/                   # MQTT 3.x 消息
│   │   │   ├── Mqtt3Connect.java
│   │   │   ├── Mqtt3ConnAck.java
│   │   │   ├── Mqtt3Publish.java
│   │   │   ├── Mqtt3PubAck.java
│   │   │   ├── Mqtt3PubRec.java
│   │   │   ├── Mqtt3PubRel.java
│   │   │   ├── Mqtt3PubComp.java
│   │   │   ├── Mqtt3Subscribe.java
│   │   │   ├── Mqtt3SubAck.java
│   │   │   ├── Mqtt3Unsubscribe.java
│   │   │   └── Mqtt3TopicFilter.java
│   │   │
│   │   └── internal/                  # MQTT 3.x 内部实现
│   │       ├── DefaultMqtt3Client.java
│   │       ├── Mqtt3AsyncClientImpl.java
│   │       ├── Mqtt3RxClientImpl.java
│   │       ├── Mqtt3BlockingClientImpl.java
│   │       ├── config/
│   │       │   └── Mqtt3ClientConfigImpl.java
│   │       ├── handler/
│   │       │   ├── Mqtt3ChannelInitializer.java
│   │       │   ├── Mqtt3ConnectHandler.java
│   │       │   ├── Mqtt3DisconnectHandler.java
│   │       │   ├── Mqtt3SubscriptionHandler.java
│   │       │   ├── Mqtt3IncomingQosHandler.java
│   │       │   └── Mqtt3OutgoingQosHandler.java
│   │       └── codec/
│   │           ├── Mqtt3MessageEncoder.java
│   │           └── Mqtt3MessageDecoder.java
│   │
│   ├── v5/                            # ===== MQTT 5（预留）=====
│   │   ├── Mqtt5Client.java           # 接口占位
│   │   ├── Mqtt5AsyncClient.java
│   │   ├── Mqtt5RxClient.java
│   │   ├── Mqtt5BlockingClient.java
│   │   │
│   │   └── message/                   # MQTT 5 消息占位
│   │       ├── Mqtt5Connect.java
│   │       ├── Mqtt5ConnAck.java
│   │       ├── Mqtt5Publish.java
│   │       └── Mqtt5Subscribe.java
│   │
│   ├── message/                       # 版本无关消息类型
│   │   ├── MqttMessage.java           # 统一消息包装
│   │   ├── QoS.java                   # QoS 枚举
│   │   └── MqttTopicFilter.java       # 版本无关主题过滤器接口
│   │
│   ├── lifecycle/                     # 版本无关的生命周期
│   │   ├── MqttClientConnectedListener.java
│   │   ├── MqttClientDisconnectedListener.java
│   │   ├── MqttClientConnectedContext.java
│   │   ├── MqttClientDisconnectedContext.java
│   │   └── MqttClientReconnector.java
│   │
│   └── internal/                      # 版本无关内部实现
│       ├── reconnect/
│       │   └── MqttAutoReconnect.java
│       ├── buffer/
│       │   └── MessageBuffer.java
│       └── util/
│           ├── PacketIdManager.java
│           └── NettyUtil.java
```

## 12. 第一版范围

### 包含
- MQTT 3.1.1 完整客户端（`plus.jmqx.client.mqtt.v3` 包，含 `message`、`internal/handler`、`internal/codec`）
- MQTT 5 包结构预留（`plus.jmqx.client.mqtt.v5`，仅接口占位，无实现）
- TCP 传输
- QoS 0/1/2 完整实现
- 三种 API（Async/Reactive/Blocking）
- 自动重连（指数退避 + jitter）
- 断线缓存
- 线程模型（Netty EventLoop + 业务线程池）
- Builder 链式 API（`.useMqttVersion3()` / `.useMqttVersion5()`）
- 连接/断开生命周期监听器

### 第二版（后续）
- MQTT 5 支持
- SSL/TLS
- WebSocket
- MQTT 3.1 兼容

### 不含（YAGNI）
- 代理（SOCKS）
- Epoll 原生传输
- 消息拦截器链
- 高级统计/指标

## 13. 依赖

```xml
<!-- 已有依赖 -->
reactor-netty              MqttRxClient 传输层
netty-codec-mqtt           MQTT 协议编解码
reactor-core               Reactive 核心
lombok                     Bean 工具
slf4j-api                  日志
hutool-all                 工具类

<!-- 无需新增依赖（无 Dagger / 无 RxJava） -->
```
