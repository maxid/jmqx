# jmqx-client MQTT Client 设计文档

- **日期**: 2026-07-02
- **状态**: 设计阶段
- **对标**: HiveMQ MQTT Client

## 1. 概述

jmqx-client 是一个基于 Netty NIO 的高性能异步 MQTT 客户端库，对标 HiveMQ MQTT Client。提供 CompletableFuture、Reactor、Blocking 三种 API 视图，支持 MQTT 3.1/3.1.1（后续扩展 MQTT 5），内置自动重连、背压、断线缓存等企业级特性。

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

### 2.2 核心接口

```java
// === 主入口 ===
public interface MqttClient {
    MqttClientConfig getConfig();
    MqttClientState getState();
    MqttAsyncClient toAsync();
    MqttRxClient toRx();
    MqttBlockingClient toBlock();

    static MqttClientBuilder builder();
}

// === Async API (CompletableFuture) ===
public interface MqttAsyncClient extends MqttClient {
    CompletableFuture<MqttConnAck> connect();
    CompletableFuture<MqttSubAck> subscribe(MqttSubscribe sub, Consumer<MqttPublish> callback);
    CompletableFuture<MqttPublishResult> publish(MqttPublish publish);
    CompletableFuture<Void> unsubscribe(MqttUnsubscribe unsub);
    CompletableFuture<Void> disconnect();
}

// === Reactive API (Reactor) ===
public interface MqttRxClient extends MqttClient {
    Mono<MqttConnAck> connect();
    Mono<MqttSubAck> subscribe(MqttSubscribe sub);
    Flux<MqttPublish> subscribePublishes(MqttSubscribe sub);
    Flux<MqttPublish> publishes(MqttGlobalPublishFilter filter);
    Mono<MqttPublishResult> publish(MqttPublish publish);
    Mono<Void> unsubscribe(MqttUnsubscribe unsub);
    Mono<Void> disconnect();
}

// === Blocking API ===
public interface MqttBlockingClient extends MqttClient {
    MqttConnAck connect();
    MqttSubAck subscribe(MqttSubscribe sub);
    MqttPublishes publishes(MqttGlobalPublishFilter filter);
    void publish(MqttPublish publish);
    void unsubscribe(MqttUnsubscribe unsub);
    void disconnect();
}
```

### 2.3 Builder API

```java
MqttClient client = MqttClient.builder()
    .serverHost("localhost")
    .serverPort(1883)
    .identifier("my-client")
    .sslWithDefaultConfig()
    .webSocketConfig(cfg -> cfg.serverPath("/mqtt"))
    .automaticReconnect()
        .initialDelay(1, TimeUnit.SECONDS)
        .maxDelay(120, TimeUnit.SECONDS)
        .backToNormalDelay(30, TimeUnit.SECONDS)
        .apply()
    .willPublish()
        .topic("last/will")
        .payload(b"offline")
        .qos(QoS.AT_LEAST_ONCE)
        .retain(true)
        .apply()
    .addConnectedListener(event -> log.info("connected"))
    .addDisconnectedListener(event -> log.warn("disconnected: {}", event.getCause()))
    .buildAsync();  // 或 buildRx() / buildBlocking() / build()
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

```java
// 核心消息类型
public enum MqttVersion { MQTT_3_1, MQTT_3_1_1 }
public enum QoS { AT_MOST_ONCE, AT_LEAST_ONCE, EXACTLY_ONCE }

// 发布消息
public class MqttPublish {
    String topic;
    byte[] payload;
    QoS qos;
    boolean retain;
    int packetId;  // 服务端自动分配
}

// 订阅
public class MqttSubscribe {
    List<MqttTopicFilter> topicFilters;
}

// 消息过滤器
public class MqttTopicFilter {
    String topicFilter;   // 支持 + 和 #
    QoS qos;
}

// 确认结果
public interface MqttConnAck {
    boolean sessionPresent();
    MqttVersion version();
}

public interface MqttSubAck {
    List<QoS> grantedQos();
}

public interface MqttPublishResult {
    MqttPublish getPublish();
    Throwable getError();  // null 表示成功
}
```

## 11. 文件结构

```
jmqx-client/src/main/java/plus/jmqx/client/
├── annotations/
│   ├── CheckReturnValue.java
│   └── Immutable.java
├── mqtt/
│   ├── MqttClient.java            # 主接口
│   ├── MqttClientBuilder.java      # 构建器
│   ├── MqttClientConfig.java       # 配置
│   ├── MqttClientState.java        # 状态枚举
│   ├── MqttVersion.java            # MQTT 版本
│   ├── MqttAsyncClient.java        # Future API
│   ├── MqttRxClient.java           # Reactor API
│   ├── MqttBlockingClient.java     # Blocking API
│   ├── MqttGlobalPublishFilter.java# 入站过滤器
│   ├── MqttPublishResult.java      # 发布结果
│   ├── MqttWebSocketConfig.java
│   ├── MqttSslConfig.java
│   ├── MqttAutoReconnectConfig.java
│   │
│   ├── message/
│   │   ├── MqttConnect.java        # CONNECT 消息
│   │   ├── MqttConnAck.java        # CONNACK
│   │   ├── MqttPublish.java        # PUBLISH 消息
│   │   ├── MqttSubscribe.java      # SUBSCRIBE
│   │   ├── MqttSubAck.java         # SUBACK
│   │   ├── MqttUnsubscribe.java    # UNSUBSCRIBE
│   │   ├── MqttDisconnect.java     # DISCONNECT
│   │   └── MqttTopicFilter.java    # 主题过滤器
│   │
│   ├── lifecycle/
│   │   ├── MqttClientConnectedListener.java
│   │   ├── MqttClientDisconnectedListener.java
│   │   ├── MqttClientConnectedContext.java
│   │   ├── MqttClientDisconnectedContext.java
│   │   └── MqttClientReconnector.java
│   │
│   └── internal/
│       ├── DefaultMqttClient.java       # 核心实现
│       ├── MqttClientImpl.java          # 引擎实现
│       ├── MqttAsyncClientImpl.java     # Future 包装
│       ├── MqttBlockingClientImpl.java  # 阻塞包装
│       ├── config/
│       │   └── MqttClientConfigImpl.java
│       ├── handler/
│       │   ├── MqttChannelInitializer.java
│       │   ├── MqttConnectHandler.java
│       │   ├── MqttDisconnectHandler.java
│       │   ├── MqttSubscriptionHandler.java
│       │   ├── MqttIncomingQosHandler.java
│       │   └── MqttOutgoingQosHandler.java
│       ├── reconnect/
│       │   ├── MqttAutoReconnect.java
│       │   └── MqttReconnectorImpl.java
│       ├── buffer/
│       │   └── MessageBuffer.java
│       └── util/
│           └── PacketIdManager.java     # packetId 分配
```

## 12. 第一版范围

### 包含
- MQTT 3.1.1（CONNECT/PUBLISH/SUBSCRIBE/UNSUBSCRIBE/DISCONNECT/PINGREQ）
- TCP 传输
- QoS 0/1/2 完整实现
- 三种 API（Async/Reactive/Blocking）
- 自动重连（指数退避 + jitter）
- 断线缓存
- 线程模型（Netty EventLoop + 业务线程池）
- Builder 链式 API
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
