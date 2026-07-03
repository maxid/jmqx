# jmqx-client MQTT Client 设计文档

- **日期**: 2026-07-02
- **状态**: 已定稿（重做版，替换旧设计）
- **对标**: HiveMQ MQTT Client
- **技术栈**: reactor-netty + netty-codec-mqtt + Project Reactor，JDK 17

> 本文档是全面重做版本。旧设计用原生 Netty（非 reactor-netty）、v5 仅占位、背压未实现、多处死锁 bug，已被废弃。本版基于 reactor-netty 单引擎双协议适配，v3.1.1 与 v5.0 同级实现，双向背压，修复所有已知问题。

## 1. 概述

jmqx-client 是一个基于 **reactor-netty** 的高性能异步 MQTT 客户端库，对标 HiveMQ MQTT Client。采用**单引擎双协议适配**架构：一个 reactor-netty 连接引擎，内部通过 `MqttMessageService` 适配 MQTT 3.1.1 与 5.0 的 wire-format 差异。提供 CompletableFuture、Reactor、Blocking 三种 API 视图，内置自动重连、双向背压、断线缓存等企业级特性。

### 设计目标

- **真正基于 reactor-netty**：`TcpClient`/`HttpClient`/`Connection` 全程异步，无 `.sync()`、无 `Thread.sleep()`、无阻塞调用
- **单引擎双协议**：QoS 状态机、重连、背压、断线缓存只写一遍，v3/v5 共享；v5 是 v3 的 Properties 超集
- **双向背压**：入站用 Reactor `request(n)` 门控 MQTT ACK；出站用信号量限 inflight
- **v3~v5 完整实现**：v5 不是占位，Properties / Reason Code / Receive Maximum 全部实现
- **全传输**：TCP / SSL/TLS / WebSocket / WSS，reactor-netty 原生支持
- **对等 HiveMQ**：功能特性、API 设计、稳定性对标 HiveMQ MQTT Client
- **与 jmqx-broker 对称**：broker 用 reactor-netty `TcpServer`/`DisposableServer`，client 用 `TcpClient`/`Connection`

### 设计决策（已与用户确认）

| 决策点 | 选择 | 理由 |
|---|---|---|
| 旧文档处理 | 全面重做 | 旧设计用原生 Netty、v5 占位、背压未做、死锁 bug |
| v3/v5 架构 | 单引擎双协议适配 | 代码最少最易维护，与 HiveMQ 内部架构一致 |
| 背压语义 | 双向背压 | 入站 request 门控 ACK + 出站 inflight 信号量 |
| 断线缓存 | 内存队列 | 覆盖 90% 场景，与 HiveMQ 默认一致 |
| 传输范围 | TCP+SSL+WS+WSS | reactor-netty 原生支持，覆盖生产场景 |

## 2. 分层架构

```
┌──────────────────────── API 层 (三视图，薄包装) ────────────────────────┐
│  Mqtt3AsyncClient (CF)   Mqtt3RxClient (Reactor)   Mqtt3BlockingClient │
│  Mqtt5AsyncClient (CF)   Mqtt5RxClient (Reactor)   Mqtt5BlockingClient │
│         ↓ 委托                  ↓ 直接实现             ↓ 委托            │
└──────────────────────────────────┬─────────────────────────────────────┘
                                   ▼
┌──────────────────── 协议适配层 (v3/v5 适配) ────────────────────────────┐
│  Mqtt3MessageService  ←→  Mqtt5MessageService   (都实现 MqttMessageService)│
│   差异点：CONNECT/CONNACK/PUBLISH 的 Properties、DISCONNECT 原因码、      │
│           SUBACK/UNSUBACK 返回码、PUBACK reason。v3 = v5 的无 Properties 子集 │
└──────────────────────────────────┬─────────────────────────────────────┘
                                   ▼
┌──────────────────────── 核心引擎层 (单引擎, 版本无关) ──────────────────┐
│  DefaultMqttClient  (implements MqttRxClient, 唯一的连接/状态机/QoS 实现) │
│   ├── MqttConnection        reactor-netty Connection 生命周期 (connect/disconnect) │
│   ├── MqttInbox             入站 Flux + 双向背压 (request → PUBACK/PUBREC 门控) │
│   ├── MqttOutbox           出站 inflight 槽位 (Receive Maximum + 背压)     │
│   ├── AckTracker           packetId → Sinks.One<Outcome> (QoS1/2 出站)      │
│   ├── InboundQos           QoS1 PUBACK / QoS2 PUBREC→PUBCOMP (受 Inbox 背压门控) │
│   ├── SubscriptionStore    topicFilter → Sinks.Many<MqttPublish> (重连恢复)  │
│   └── MessageBuffer        断线缓存 (offer/flush 时重注 AckTracker)          │
└──────────────────────────────────┬─────────────────────────────────────┘
                                   ▼
┌──────────────────────── 传输层 (reactor-netty) ──────────────────────────┐
│  TcpClient (TCP / TLS) · HttpClient (WS / WSS)                           │
│  Pipeline: MqttEncoder → MqttDecoder(8MB,autodetect v3/v5) → 业务handlers │
└─────────────────────────────────────────────────────────────────────────┘
```

### 关键纠正点（对照旧设计）

- **真正用 reactor-netty**：`TcpClient.connect()` 返回 `Mono<Connection>`，全程异步，无任何 `.sync()`/`Thread.sleep()`。`Connection.outbound().send()` 写出站，`Connection.inbound().receiveObject()` 驱动入站。
- **单引擎**：`DefaultMqttClient` 只有一个，持有 `MqttMessageService`（v3 或 v5 实现）。QoS 状态机、重连、背压、断线缓存只写一遍，v3/v5 共享。
- **背压贯穿**：`MqttInbox` 用 Reactor `Flux` 的 `request(n)` 控制「向下游推 N 条后暂停发 PUBACK/PUBREC」；`MqttOutbox` 用信号量限制 inflight，满了 `publish()` 的 `Mono` 挂起而非报错。
- **v5 同级实现**：不是占位，`Mqtt5MessageService` + Properties 全部实现。
- **单 Netty handler**：旧设计散落三个 `ChannelDuplexHandler`（Subscription/IncomingQos/OutgoingQos），wiring 混乱。改为单一 `MqttClientHandler` 统一收发，内部委托给普通 Java 对象（`AckTracker`/`InboundQos`/`MqttInbox`），可独立单测。

## 3. 版本化 API

按 MQTT 版本划分接口，顶部 `MqttClient` 提供版本选择入口：

```java
// 版本无关的入口
public interface MqttClient {
    MqttClientConfig getConfig();
    MqttClientState getState();
    MqttVersion getVersion();
    static MqttClientBuilder builder();
}

// MQTT 3.1.1 客户端接口
public interface Mqtt3Client extends MqttClient {
    Mqtt3ClientConfig getConfig();
    Mqtt3AsyncClient toAsync();
    Mqtt3RxClient toRx();
    Mqtt3BlockingClient toBlock();
    static Mqtt3ClientBuilder builder();
}

// MQTT 5 客户端接口（同级实现，非占位）
public interface Mqtt5Client extends MqttClient {
    Mqtt5ClientConfig getConfig();
    Mqtt5AsyncClient toAsync();
    Mqtt5RxClient toRx();
    Mqtt5BlockingClient toBlock();
    static Mqtt5ClientBuilder builder();
}

// Async API (CompletableFuture)
public interface Mqtt3AsyncClient extends Mqtt3Client {
    CompletableFuture<Mqtt3ConnAck> connect();
    CompletableFuture<Mqtt3SubAck> subscribe(Mqtt3Subscribe sub, Consumer<Mqtt3Publish> callback);
    CompletableFuture<Mqtt3PublishResult> publish(Mqtt3Publish publish);
    CompletableFuture<Void> unsubscribe(Mqtt3Unsubscribe unsub);
    CompletableFuture<Void> disconnect();
}

// Reactive API (Reactor) — DefaultMqttClient 直接实现此接口
public interface Mqtt3RxClient extends Mqtt3Client {
    Mono<Mqtt3ConnAck> connect();
    Mono<Mqtt3SubAck> subscribe(Mqtt3Subscribe sub);
    Flux<Mqtt3Publish> subscribePublishes(Mqtt3Subscribe sub);
    Flux<Mqtt3Publish> publishes(MqttGlobalPublishFilter filter);
    Mono<Mqtt3PublishResult> publish(Mqtt3Publish publish);
    Mono<Void> unsubscribe(Mqtt3Unsubscribe unsub);
    Mono<Void> disconnect();
}

// Blocking API
public interface Mqtt3BlockingClient extends Mqtt3Client {
    Mqtt3ConnAck connect();
    Mqtt3SubAck subscribe(Mqtt3Subscribe sub);
    void publish(Mqtt3Publish publish);
    void unsubscribe(Mqtt3Unsubscribe unsub);
    void disconnect();
}
```

v5 接口同构（`Mqtt5AsyncClient`/`Mqtt5RxClient`/`Mqtt5BlockingClient`），方法签名用 `Mqtt5ConnAck`/`Mqtt5Publish` 等带 Properties 的类型。

## 4. 协议适配层与消息模型

消息对象是**值类型**（接口 + 不可变实现），构建器返回不可变实例。v3 接口直接继承版本无关接口，v5 接口扩展加 Properties。

### 版本无关消息接口（`plus.jmqx.client.mqtt.message`）

```java
public interface MqttPublish {
    String getTopic(); byte[] getPayloadAsBytes(); QoS getQoS();
    boolean isRetain(); boolean isDup(); int getPacketId();
}
public interface MqttConnect {
    String getClientId(); boolean isCleanSession(); int getKeepAliveSeconds();
    MqttPublish getWillPublish(); String getUsername(); byte[] getPassword();
}
public interface MqttSubscribe { List<? extends MqttTopicFilter> getTopicFilters(); int getPacketId(); }
public interface MqttTopicFilter { String getTopicFilter(); QoS getQoS(); }
public interface MqttUnsubscribe { List<String> getTopicFilters(); int getPacketId(); }
public interface MqttConnAck { boolean isSessionPresent(); }
public interface MqttSubAck { List<QoS> getGrantedQos(); }
public interface MqttPublishResult { MqttPublish getPublish(); Throwable getError(); }
public enum QoS { AT_MOST_ONCE(0), AT_LEAST_ONCE(1), EXACTLY_ONCE(2); }
```

### 版本化子包

`v3.message` 与 `v5.message` 各有一套接口，**v5 接口多出 Properties / Reason Code**：

```java
// v5 独有 (plus.jmqx.client.mqtt.v5.message)
public interface Mqtt5Publish extends MqttPublish {
    Mqtt5PublishProperties getProperties();   // ResponseTopic, CorrelationData, UserProps, MsgExpiry...
}
public interface Mqtt5Connect extends MqttConnect {
    int getReceiveMaximum(); boolean isCleanStart(); long getSessionExpiryInterval();
}
public interface Mqtt5ConnAck extends MqttConnAck {
    byte getReasonCode(); Mqtt5ConnAckProperties getProperties(); // ReceiveMax, ServerKeepAlive...
}
// v3 接口: Mqtt3Publish extends MqttPublish（不加字段）
```

### MqttMessageService — 协议适配核心

```java
// plus.jmqx.client.mqtt.internal (版本无关接口)
public interface MqttMessageService {
    // 编码：业务消息 → netty MqttMessage
    io.netty.handler.codec.mqtt.MqttMessage encodeConnect(MqttConnect);
    io.netty.handler.codec.mqtt.MqttMessage encodePublish(MqttPublish);
    io.netty.handler.codec.mqtt.MqttMessage encodeSubscribe(MqttSubscribe);
    io.netty.handler.codec.mqtt.MqttMessage encodeUnsubscribe(MqttUnsubscribe);
    io.netty.handler.codec.mqtt.MqttMessage encodePubAck(int packetId);
    io.netty.handler.codec.mqtt.MqttMessage encodePubRec(int packetId);
    io.netty.handler.codec.mqtt.MqttMessage encodePubRel(int packetId);
    io.netty.handler.codec.mqtt.MqttMessage encodePubComp(int packetId);
    io.netty.handler.codec.mqtt.MqttMessage encodeDisconnect();
    io.netty.handler.codec.mqtt.MqttMessage encodePingReq();
    // 解码：netty 入站消息 → 业务消息
    MqttConnAck decodeConnAck(MqttConnAckMessage);
    MqttPublish decodePublish(MqttPublishMessage);
    MqttSubAck decodeSubAck(MqttSubAckMessage);
    int decodePacketId(MqttMessage);
    // 原因码/错误解释
    boolean isConnectionAccepted(MqttConnAck);
    RuntimeException connectionRefusedException(MqttConnAck);
}
```

两个实现：
- `Mqtt3MessageService`（`v3.internal`）：用 `MqttVersion.MQTT_3_1_1`，无 Properties，CONNACK 用 0~5 returnCode 映射。
- `Mqtt5MessageService`（`v5.internal`）：用 `MqttVersion.MQTT_5`，CONNECT/CONNACK/PUBLISH 带完整 `MqttProperties`，DISCONNECT/PUBACK 带 reason code，`cleanStart` 替代 `cleanSession`，`sessionExpiryInterval`。

**单引擎双协议的关键**：`DefaultMqttClient` 只依赖 `MqttMessageService` 抽象；构造时注入 v3 或 v5 实现。QoS 状态机、重连、背压代码完全版本无关。

### 消息构建器（对齐 HiveMQ 流式 API）

```java
Mqtt3Publish.builder().topic("t").payload(bytes).qos(QoS.AT_LEAST_ONCE).retain(false).build();
Mqtt3Subscribe.builder().topicFilter("t/#", QoS.AT_MOST_ONCE).addTopicFilter("a/+", QoS.AT_LEAST_ONCE).build();
Mqtt5Publish.builder().topic("t").payload(bytes).qos(QoS.EXACTLY_ONCE)
    .properties(Mqtt5PublishProperties.builder().responseTopic("t/resp").userProp("k","v").build())
    .build();
```

## 5. 连接引擎与状态机

### 连接生命周期（reactor-netty 全异步）

```java
private Mono<Connection> doConnect() {
    return transportFactory.tcpClient(config)        // 按 ssl/ws 选 TcpClient 或 HttpClient(ws)
        .connect()                                   // 返回 Mono<Connection>，非阻塞
        .flatMap(conn -> {
            this.connection = conn;
            installPipeline(conn);                    // 挂 MqttEncoder/MqttDecoder/业务handler (doOnConnected 已挂)
            Sinks.One<MqttConnAck> ackSink = Sinks.one();
            conn.outbound().sendObject(messageService.encodeConnect(connectMsg))
                .then().subscribe();                  // 异步写出 CONNECT
            conn.inbound().receiveObject()            // 异步读入站
                .cast(io.netty.handler.codec.mqtt.MqttMessage.class)
                .doOnNext(msg -> onInbound(msg, ackSink))
                .doOnError(this::onTransportError)
                .subscribe(this.inboundSubscription);  // 喂入 MqttInbox/AckTracker
            return ackSink.asMono();                  // CONNACK 回来才完成
        });
}
```

**没有 `.sync()`，没有 `Thread.sleep()`**：连接、写出、读入全是 reactor 异步。`transportFactory` 是唯一决定传输类型的组件。

### 状态机（4 态，对齐 HiveMQ）

旧设计只有 3 态（无 DISCONNECTING），导致自动重连无法与「用户主动断开」区分。

```
         ┌──────────────────────────────────────────────────────┐
         │                                                      │
         ▼                                                      │
   ┌────────────┐  connect()  ┌────────────┐ CONNACK ok  ┌───────────┐
   │DISCONNECTED│────────────►│ CONNECTING │────────────►│ CONNECTED│
   └─────┬──────┘             └─────┬──────┘             └─────┬─────┘
         │                          │                          │ │
         │                          │ CONNACK fail/            │ │ 意外断开
         │                          │ transport error          │ ▼
         │                          ▼                          ▼
         │                     ┌──────────────┐  ←──────────────┘
         │                     │ DISCONNECTING │
         │                     └──────┬───────┘
         │                            │
         │  disconnect()(user)        │
         │  或重连关闭                 │
         └────────────────────────────┘
```

状态（`MqttClientState` 枚举）：`DISCONNECTED`、`CONNECTING`、`CONNECTED`、`DISCONNECTING`

状态转换规则：
- `DISCONNECTED → CONNECTING`：调用 `connect()` 时
- `CONNECTING → CONNECTED`：收到 CONNACK 成功后
- `CONNECTING → DISCONNECTING`：CONNACK 失败或 transport error
- `CONNECTED → DISCONNECTING`：意外断开
- `* → DISCONNECTED`：主动 `disconnect()` 完成或重连关闭清理完毕

状态转换全部走 `AtomicReference<MqttClientState>.compareAndSet()`，CAS 失败则拒绝操作并返回错误 `Mono`（不抛同步异常）。

### connect() 的 Reactor 契约

```java
public Mono<MqttConnAck> connect() {
    return Mono.defer(() -> {
        if (!state.compareAndSet(DISCONNECTED, CONNECTING))
            return Mono.error(new IllegalStateException("Client is " + state.get()));
        return doConnect()
            .doOnSuccess(ack -> {
                state.set(CONNECTED); reconnectAttempts = 0;
                notifyConnected(ack.isSessionPresent());
                resubscribe();                  // 重连后恢复订阅
                flushMessageBuffer();           // 重连后刷断线缓存
            })
            .doOnError(err -> {
                state.set(DISCONNECTING);
                handleUnexpectedDisconnect(err); // 通知 listener + 触发重连
            });
    });
}
```

### Keepalive — PINGREQ/PINGRESP（旧设计完全缺失）

broker 会在 1.5×keepAlive 后判定掉线。客户端必须主动 ping：

- reactor-netty `Connection` 用 `IdleStateHandler`(read=1.5×keepAlive, write=keepAlive, all=0) 监听读写空闲
- 写空闲触发时，若状态为 CONNECTED，发出 `PINGREQ`
- 读超时（无 PINGRESP）则判定连接断开 → 走 `handleUnexpectedDisconnect`
- PINGRESP 入站时重置读超时计时

## 6. QoS 流程与 ACK 跟踪

旧设计有两个致命 bug：① 两个独立的 `PacketIdManager`（client 一个、SubscriptionHandler 一个）导致 packetId 碰撞；② 断线缓存 flush 时不重注 ACK 跟踪，QoS1/2 缓存消息重发后结果永远不完成。

### 单一 PacketIdManager（修复 bug ①）

整个 `DefaultMqttClient` 持有**唯一** `PacketIdManager`，所有出站报文（PUBLISH Q1/2、SUBSCRIBE、UNSUBSCRIBE）共用。`SubscriptionStore` 不再自己分配 packetId，而是由 client 传入。

```java
public final class PacketIdManager {
    private final AtomicInteger next = new AtomicInteger(1);
    private final int max = 65535;
    public int nextPacketId() {                   // CAS 循环，溢出回绕到 1
        int id;
        do { id = next.getAndIncrement(); if (id > max) next.compareAndSet(id+1, 1); }
        while (id == 0);                          // 0 保留，跳过
        return id;
    }
}
```

### 出站 QoS 与 AckTracker（修复 bug ②）

`AckTracker` 是 `packetId → PendingOutbound` 的映射，`PendingOutbound` 封装原始 PUBLISH + 结果 `Sinks.One<Mqtt3PublishResult>` + 发送时间（用于重发 DUP）。消息是不可变值类型，修改 packetId 用 `toBuilder()` 生成新实例：

```java
public Mono<Mqtt3PublishResult> publish(Mqtt3Publish publish) {
    return Mono.defer(() -> {
        if (state.get() == CONNECTED) {
            if (publish.getQoS() == AT_MOST_ONCE)
                return sendQos0(publish);                 // 不占 packetId，不跟踪
            int pid = packetIdManager.nextPacketId();
            publish = publish.toBuilder().packetId(pid).build();  // 不可变 → toBuilder 重建
            var pending = new PendingOutbound(publish, Sinks.one());
            return MqttOutbox.acquire(pid, pending)        // inflight 槽位（§7 背压）
                .then(sendPublish(pending))               // 写 PUBLISH + 注册 tracker
                .then(pending.result().asMono());          // ACK 来才 complete
        }
        if (state.get() == DISCONNECTED && config.isAutomaticReconnect())
            return bufferOffline(publish);                 // 进断线缓存（§8）
        return Mono.error(new IllegalStateException("Client is " + state.get()));
    });
}
```

`AckTracker` 入站分发（PUBACK/PUBREC/PUBCOMP）：

```java
void onInboundAck(io.netty.handler.codec.mqtt.MqttMessage msg) {
    int pid = codec.decodePacketId(msg);
    switch (msg.fixedHeader().messageType()) {
        case PUBACK -> tracker.complete(pid, ok());        // QoS1 完成
        case PUBREC -> {                                    // QoS2 阶段1
            tracker.markReceived(pid);                      // 保留槽位，进入阶段2
            send(PUBREL, pid);                             // 回 PUBREL
        }
        case PUBCOMP -> tracker.complete(pid, ok());        // QoS2 阶段2 完成
    }
}
```

### 入站 QoS（受 Inbox 背压门控）

入站 PUBLISH 的 ACK **不由 QoS handler 立即发**，而是交给 `MqttInbox` 的背压门控（§7）。这里只做协议状态维护：

```java
void onInboundPublish(MqttPublishMessage nettyMsg) {
    MqttPublish pub = messageService.decodePublish(nettyMsg);
    switch (pub.getQoS()) {
        case AT_MOST_ONCE -> inbox.deliver(pub);           // 直发，无需 ACK
        case AT_LEAST_ONCE -> inbox.deliver(pub, () -> send(PUBACK, pid));  // 消费后回调发 ACK
        case EXACTLY_ONCE -> {
            inbox.deliver(pub, () -> send(PUBREC, pid));   // 消费后发 PUBREC
            trackPubRel(pid);                              // 等 broker PUBREL
        }
    }
}
void onInboundPubRel(int pid) { send(PUBCOMP, pid); }       // 阶段2
```

关键：ACK 发送是「消费后回调」而非「收到立即发」，这样下游 `request(n)` 才能真正施加 MQTT 流控背压。

### QoS 流程总览

| 方向 | QoS | 流程 |
|---|---|---|
| 出站 (C→B) | 0 | `PUBLISH` → 直接成功，无需等待 |
| 出站 (C→B) | 1 | `PUBLISH` → 等待 `PUBACK` → 完成 Mono |
| 出站 (C→B) | 2 | `PUBLISH` → 等待 `PUBREC` → `PUBREL` → 等待 `PUBCOMP` → 完成 |
| 入站 (B→C) | 0 | 直接回调 subscription handler |
| 入站 (B→C) | 1 | `PUBLISH` → 回调 → **消费后**回复 `PUBACK` |
| 入站 (B→C) | 2 | `PUBLISH` → 回调 → **消费后**回复 `PUBREC` → 等待 `PUBREL` → 回复 `PUBCOMP` |

## 7. 双向背压

背压分两向，核心思想：**用 MQTT 自身的 ACK 机制做天然流控，而非另起一套队列限流**。

### 入站背压（broker → client）：request 门控 ACK

`MqttInbox` 是 `Sinks.Many<MqttPublish>` 的封装。每个订阅的下游 `Flux` 由订阅者 `request(n)` 驱动。关键：**ACK 的发送推迟到下游真正消费之后**。

```java
class MqttInbox {
    private final Map<MqttTopicFilter, Sinks.Many<MqttPublish>> subscriptionSinks;
    private final Sinks.Many<MqttPublish> globalSink;

    void onPublish(MqttPublish pub, int packetId, Runnable ackAction) {
        Sinks.Many<MqttPublish> sink = routeToSink(pub);   // 匹配的订阅 sink
        EmitResult r = sink.tryEmitNext(withAck(pub, ackAction));
        // ↑ tryEmitNext 受 Sinks.Many 的缓冲容量限制 = 天然背压起点
        if (r.isFailure()) {
            // sink 满（订阅者慢）→ 暂停 ACK，消息留在 broker 侧（QoS1/2）
            // QoS0 此时只能 drop + 计数告警（无 ACK 可暂停）
            metrics.droppedQos0();
        }
        // ackAction 不在这里调用 → 推迟到下游消费
    }
}
```

下游消费时才触发 ACK：

```java
client.subscribePublishes(sub)
    .doOnNext(pub -> {
        process(pub);
        pub.ack();        // ← 真正发 PUBACK(QoS1)/PUBREC(QoS2)；QoS0 是 no-op
    })
    .subscribe();
// subscriber.request(10) → Sinks.Many 下游拉 10 条 → 每消费一条调 ack() → broker 收到 ACK 才推下一条
// 下游不 request → 不 ack → broker（QoS1/2）不再推 → 完美背压
```

**Sinks.Many 容量**：用 `Sinks.many().multicast().onBackpressureBuffer(capacity, false)`，capacity 默认 1024（`inboxBufferSize`）。满了 `tryEmitNext` 失败 → 暂停 ACK。配合 keepalive，broker 不会因没收到 ACK 而判定客户端掉线（ACK 延迟是合法的，只要 keepalive 在）。

### 出站背压（client → broker）：inflight 信号量

`MqttOutbox` 用 `Semaphore` 限制并发未确认 PUBLISH：

```java
class MqttOutbox {
    private final Semaphore inflightSlots;   // permits = ReceiveMaximum(v5) 或 maxInflight(v3)
    private final Map<Integer, PendingOutbound> pending = new ConcurrentHashMap<>();
    private final Queue<Waiter> pendingQueue;  // 满时排队

    Mono<Void> acquire(int pid, PendingOutbound p) {
        return Mono.<Void>create(sink -> {
            if (inflightSlots.tryAcquire()) {
                pending.put(pid, p); sink.success();
            } else {
                pendingQueue.offer(new Waiter(pid, p, sink));  // 挂起，不报错
            }
        });
    }
    void release(int pid) {  // ACK 到达时调用
        pending.remove(pid);
        inflightSlots.release();
        Waiter w = pendingQueue.poll();
        if (w != null) { pending.put(w.pid, w.p); w.sink.success(); }
    }
}
```

ACK 到达 → `inflightSlots.release()` → 唤醒一个排队的 Waiter。`publish()` 的 `Mono` 在 inflight 满时**挂起不报错**，等槽位释放后继续。

### 两向背压联动

```
慢消费场景：
  broker 推 1000 条 QoS1 → 客户端订阅者只 request(10)
  → inbox sink 缓冲满 → 暂停发 PUBACK
  → broker 收不到 ACK → 按 MQTT 协议停止推新消息（窗口机制）
  → 客户端不 OOM，broker 不丢消息

快生产场景：
  客户端 publish 10000 条 QoS1 → broker Receive Maximum=100
  → 100 个 inflight 占满 → publish() Mono 挂起
  → broker 处理完一个回 PUBACK → 释放一个槽 → 挂起的 publish 继续
  → 客户端不 OOM，broker 不超载
```

### v5 的额外流控：Receive Maximum 双向

v5 CONNACK 带服务端 `Receive Maximum`（限制客户端 inflight）和客户端 CONNECT 可带 `Receive Maximum`（限制服务端 inflight）。两个值都从 Properties 提取，分别喂给 `MqttOutbox` 的信号量和 `MqttInbox` 的门控阈值。v3 用配置默认值（`maxInflightMessages=64`）。

## 8. 断线缓存

旧设计的 bug：`flush()` 直接 `channel.writeAndFlush(encodePublish(pub))`，**绕过了 AckTracker**——QoS1/2 缓存消息重发后，结果 sink 永远不会 complete，且没分配新 packetId。

### MessageBuffer（修复版）

```java
class MessageBuffer {
    private final Queue<BufferedPublish> queue;   // ConcurrentLinkedQueue
    private final int maxSize;                   // 默认 1000
    private final long maxBytes;                  // 默认 64MB
    private final AtomicLong currentBytes = new AtomicLong(0);

    record BufferedPublish(Mqtt3Publish publish, Sinks.One<Mqtt3PublishResult> resultSink) {}

    Mono<Mqtt3PublishResult> offer(Mqtt3Publish publish) {
        Sinks.One<Mqtt3PublishResult> sink = Sinks.one();
        long bytes = estimateBytes(publish);
        if (currentBytes.get() + bytes > maxBytes || queue.size() >= maxSize) {
            sink.tryEmitError(new MessageBufferFullException());
            return sink.asMono();
        }
        queue.offer(new BufferedPublish(publish, sink));
        currentBytes.addAndGet(bytes);
        return sink.asMono();    // 待定，flush 时才真正完成
    }

    Mono<Void> flush() {
        return Mono.defer(() -> {
            BufferedPublish bp;
            List<Mono<Void>> sends = new ArrayList<>();
            while ((bp = queue.poll()) != null) {
                currentBytes.addAndGet(-estimateBytes(bp.publish()));
                // 关键：委托给 DefaultMqtt3Client.publish()，重新分配 packetId + 注册 AckTracker
                sends.add(
                    client.publish(bp.publish())
                        .doOnNext(result -> bp.resultSink().tryEmitValue(result))
                        .doOnError(err -> bp.resultSink().tryEmitError(err))
                        .then()
                );
            }
            return Flux.concat(sends).then();   // 按原始顺序
        });
    }
}
```

**核心修复**：flush 时不直接写 channel，而是**调用 `client.publish()`**——这样每条缓存消息重新走完整 QoS 路径（分配 packetId、占 inflight 槽位、注册 AckTracker），ACK 到达时原始的 `resultSink` 正确 complete。

### 缓存策略边界

- `disconnect()`（用户主动）：**不触发重连**，但缓存**保留**（用户可能再 connect）。可在 config 配置 `clearBufferOnDisconnect`（默认 false）。
- 意外断开 + 开启 `automaticReconnect`：缓存保留，重连后 flush。
- 意外断开 + 关闭重连：缓存消息的 `resultSink` 全部 emit `ClientDisconnectedException`（不让调用方永久挂起）。
- 缓存满：`publish()` 立即返回 `Mono.error(MessageBufferFullException)`，不阻塞调用方。

## 9. 自动重连

旧设计 bug：用 `Thread.sleep(delayMs)` + `client.connect().subscribe()` 在 listener 回调里阻塞当前线程——这是死锁源（listener 可能在 event-loop 线程触发，sleep 阻塞它，connect 又依赖它）。

### reactor 异步重连调度

```java
class MqttAutoReconnect implements MqttClientDisconnectedListener {
    private final long initialDelayMs, maxDelayMs;
    private final DefaultMqttClient client;
    private final AtomicBoolean stopped = new AtomicBoolean(false);  // 用户 disconnect 置 true

    @Override
    public void onDisconnected(MqttClientDisconnectedContext ctx) {
        if (ctx.getSource() == USER) { stopped.set(true); return; }   // 主动断开不重连
        if (stopped.get()) return;
        scheduleReconnect(ctx.getReconnector());
    }

    private void scheduleReconnect(MqttClientReconnector r) {
        int attempt = r.getAttempts() + 1;
        long delay = computeBackoff(attempt);     // min(initial * 2^attempt, max) ± 25% jitter
        r.delay(delay);
        // 关键：用 reactor 的 Mono.delay 调度，绝不 Thread.sleep
        Mono.delay(Duration.ofMillis(delay))
            .flatMap(t -> client.connect())        // 异步重连
            .subscribe(
                ack -> log.info("Reconnected on attempt {}", attempt),
                err -> scheduleReconnect(new MqttClientReconnector(attempt, true))  // 失败递增重试
            );
    }

    private long computeBackoff(int attempt) {
        long base = Math.min(initialDelayMs * (1L << Math.min(attempt, 16)), maxDelayMs);
        double jitter = 1.0 + (ThreadLocalRandom.current().nextDouble() - 0.5) * 0.5;
        return (long)(base * jitter);
    }
}
```

**全部异步**：`Mono.delay()` 在 reactor 的 `parallel` scheduler 上计时，不占任何业务/event-loop 线程。重连成功/失败都是 reactor 回调驱动。

### 重连后的恢复序列（connect 的 doOnSuccess 已串联）

```
重连成功 (CONNECTED)
  ├─ resubscribe()        ← SubscriptionStore 里所有 topicFilter 重新发 SUBSCRIBE
  │                         每个 SUBSCRIBE 的结果 sink 用新 packetId
  ├─ flushMessageBuffer() ← §8，重注 ACK 走完整 publish() 路径
  └─ notifyConnected()    ← ConnectedListeners
```

### Session 恢复策略（v3 cleanSession / v5 sessionExpiry）

- `cleanSession=true`（v3）/ `cleanStart=true` 且 `sessionExpiry=0`（v5）：重连后**必须重订阅**（broker 不保留会话）。
- `cleanSession=false`（v3）/ session 未过期（v5）：broker 保留订阅和未送达消息，重连后 `sessionPresent=true`，**可不重订阅**，断线期间的 QoS1/2 离线消息由 broker 重投。
- 由 `MqttClientReconnector.resubscribeIfSessionPresent()` 控制，默认根据 CONNACK 的 sessionPresent 自动判断。

### 重连熔断

- `maxReconnectAttempts`（默认无限）+ `reconnectMaxDelayMs`：达到上限后停止重连，通知 `DisconnectedListener` 最终失败。
- 用户随时可 `connect()` 重新启动。

## 10. 传输层

`TransportFactory` 是唯一决定传输类型的组件，`DefaultMqttClient` 完全不感知传输细节。

### TransportFactory

```java
class TransportFactory {
    Mono<Connection> connect(MqttClientConfig config) {
        return switch (config.getTransportType()) {
            case TCP  -> tcpClient(config).connect();
            case TLS  -> applyTlsTcp(tcpClient(config), config).connect();
            case WS   -> httpClient(config).websocket().uri(wsUri(config)).connect();
            case WSS  -> applyTlsHttp(httpClient(config), config).websocket().uri(wsUri(config)).connect();
        };
    }
    // pipeline 由 MqttClientEngine 在 connect 成功后调用 installPipeline() 挂载
}
```

**WS/WSS 帧封装**：reactor-netty `HttpClient.websocket()` 握手后，`channel.writeAndFlush(MqttMessage)` 的出站路径不会自动把 MQTT 字节流封装为 WebSocket 二进制帧。客户端在 `installPipeline()` 中额外安装与 jmqx-broker 对称的帧转换器（`transport/ws/`）：

```
ws-decoder → mqttWsFrameDecoder → ws-encoder → mqttWsFrameEncoder → mqttEncoder → mqttDecoder → …
```

- 出站：`MqttEncoder` → `ByteBuf` → `ByteBufToWebSocketFrameEncoder` → `BinaryWebSocketFrame` → `ws-encoder`
- 入站：`ws-decoder` → `BinaryWebSocketFrame` → `WebSocketFrameToByteBufDecoder` → `ByteBuf` → `MqttDecoder`

若不安装上述帧封装，CONNECT 首字节 `0x10` 会被 broker 侧 WebSocket 解码器误解析为 RSV=1 帧头，导致握手后立即断连。

**与 jmqx-broker 对称**：broker 用 `TcpServer`/`DisposableServer`，client 用 `TcpClient`/`Connection`，同一套 reactor-netty。`MqttDecoder(8MB)` 自动识别 v3.1/3.1.1/5.0，无需分版本 decoder。

### MQTT Pipeline（doOnConnected 挂载）

```
MqttEncoder (outbound, 单例)
MqttDecoder (inbound, 8MB max, 自动版本探测)
IdleStateHandler (read=1.5×keepAlive, write=keepAlive, all=0)   ← keepalive
MqttClientHandler (inbound 业务分发 + outbound 写出)              ← 单一核心收发 handler
```

不使用多个 `ChannelDuplexHandler` 散落各处。改为**单一 `MqttClientHandler`** 统一收发，内部委托给 `AckTracker`/`InboundQos`/`MqttInbox`——这些是普通 Java 对象（非 Netty handler），可独立单测。

### SSL/TLS 配置

```java
.sslConfig(MqttSslConfig.builder()
    .trustStore(path).keyStore(path, pass)
    .cipherSuites(...).protocols("TLSv1.3","TLSv1.2")
    .handshakeTimeout(10_000)
    .insecureTrustAll(true)   // 仅测试环境：信任自签证书
    .build())
```

底层用 Netty `SslContextBuilder.forClient()` 构建 `SslContext`，经 reactor-netty `secure(spec -> spec.sslContext(...))` 注入。`insecureTrustAll` 使用 `InsecureTrustManagerFactory`；生产环境应配置 trustStore 或 CA。

### WebSocket

```java
.websocketConfig(MqttWebSocketConfig.builder()
    .serverHost("...").serverPort(9001)
    .path("/mqtt").subprotocol("mqtt").queryParam(...)
    .build())
```

底层 reactor-netty `HttpClient.websocket().uri("/mqtt")` 建立 WebSocket 连接；路径与子协议由 `MqttWebSocketConfig` 配置（默认 `/mqtt`、`mqtt`）。MQTT 载荷经 §10 帧封装层以 **BinaryWebSocketFrame** 传输。

## 11. 配置模型

```java
public class MqttClientConfig {
    // 连接
    String serverHost = "localhost"; int serverPort = 1883;
    String clientId;                 // null → 自动生成 "jmqx-" + UUID前8
    int keepAliveSeconds = 60;
    MqttVersion version;             // 由 builder.useMqttVersionX() 设置

    // 超时
    int socketConnectTimeoutMs = 10_000; int mqttConnectTimeoutMs = 60_000;

    // 传输
    TransportType transportType = TCP;
    MqttSslConfig sslConfig; MqttWebSocketConfig webSocketConfig;

    // 线程 (reactor-netty 用 LoopResources)
    int nettyThreads = max(cpu, 2);
    LoopResources loopResources;     // 可外部注入共享

    // 会话
    boolean cleanSession = true;      // v3; v5 映射到 cleanStart
    long sessionExpiryInterval = 0;   // v5 only
    int receiveMaximum = 65535;        // v5 only，客户端限制服务端 inflight

    // 认证
    String username; byte[] password;

    // 遗嘱
    MqttPublish willPublish;

    // 重连
    boolean automaticReconnect = false;
    long reconnectInitialDelayMs = 1000; long reconnectMaxDelayMs = 120_000;
    int maxReconnectAttempts = Integer.MAX_VALUE;

    // 缓冲 & 流控
    int messageBufferMaxSize = 1000; long messageBufferMaxBytes = 64L*1024*1024;
    boolean clearBufferOnDisconnect = false;
    int maxInflightMessages = 64;     // v3; v5 从 CONNACK Receive Maximum 覆盖
    int inboxBufferSize = 1024;       // Sinks.Many 背压缓冲
}
```

## 12. Builder API

对齐 HiveMQ 流式构建器，顶层入口选版本：

```java
// v3
Mqtt3RxClient c = MqttClient.builder()
    .useMqttVersion3()                       // → Mqtt3ClientBuilder
        .serverHost("localhost").serverPort(1883)
        .identifier("my-client")
        .keepAliveSeconds(60).cleanSession(true)
        .username("u").password("p".getBytes())
        .willPublish().topic("last/will").payload("offline".getBytes())
            .qos(QoS.AT_LEAST_ONCE).retain(true).apply()
        .sslConfig(MqttSslConfig.builder()...)
        .automaticReconnect().initialDelay(1, SECONDS).maxDelay(120, SECONDS).apply()
        .addConnectedListener(e -> log.info("connected"))
        .addDisconnectedListener(e -> log.warn("disconnected"))
    .buildRx();                              // 或 buildAsync() / buildBlocking()

// v5 同构，多出 v5 专属配置
Mqtt5RxClient c5 = MqttClient.builder().useMqttVersion5()
    .sessionExpiryInterval(300_000)
    .receiveMaximum(100)
    .cleanStart(true)
    ...buildRx();
```

v5 builder 多出 `.sessionExpiryInterval()/.receiveMaximum()/.userProperties()/.reasonCode()` 等。

## 13. 文件结构

```
jmqx-client/src/main/java/plus/jmqx/client/
├── mqtt/
│   ├── MqttClient.java                  # 主入口接口
│   ├── MqttClientBuilder.java           # 顶层构建器 (.useMqttVersion3() / .useMqttVersion5())
│   ├── MqttClientConfig.java            # 配置基类
│   ├── MqttClientState.java             # 状态枚举 (4 态)
│   ├── MqttVersion.java                 # 版本枚举
│   ├── MqttGlobalPublishFilter.java     # 入站过滤器
│   │
│   ├── message/                         # 版本无关消息接口
│   │   ├── MqttPublish.java  MqttConnect.java  MqttSubscribe.java
│   │   ├── MqttUnsubscribe.java  MqttConnAck.java  MqttSubAck.java
│   │   ├── MqttPublishResult.java  MqttTopicFilter.java  QoS.java
│   │
│   ├── lifecycle/                       # 生命周期
│   │   ├── MqttClientConnectedListener.java  MqttClientDisconnectedListener.java
│   │   ├── MqttClientConnectedContext.java  MqttClientDisconnectedContext.java
│   │   └── MqttClientReconnector.java
│   │
│   ├── v3/                              # MQTT 3.1.1
│   │   ├── Mqtt3Client.java  Mqtt3ClientBuilder.java  Mqtt3ClientConfig.java
│   │   ├── Mqtt3AsyncClient.java  Mqtt3RxClient.java  Mqtt3BlockingClient.java
│   │   ├── Mqtt3PublishResult.java
│   │   ├── message/                     # Mqtt3Publish, Mqtt3Connect, Mqtt3ConnAck, ...
│   │   └── internal/Mqtt3MessageService.java
│   │
│   ├── v5/                              # MQTT 5 (同级实现)
│   │   ├── Mqtt5Client.java  Mqtt5ClientBuilder.java  Mqtt5ClientConfig.java
│   │   ├── Mqtt5AsyncClient.java  Mqtt5RxClient.java  Mqtt5BlockingClient.java
│   │   ├── Mqtt5PublishResult.java
│   │   ├── message/                     # Mqtt5Publish(+Properties), Mqtt5Connect, Mqtt5ConnAck, ...
│   │   │   └── Mqtt5PublishProperties.java  Mqtt5ConnAckProperties.java  ...
│   │   └── internal/Mqtt5MessageService.java
│   │
│   └── internal/                        # 版本无关核心
│       ├── DefaultMqttClient.java       # 单引擎 (implements Mqtt3RxClient 或 Mqtt5RxClient)
│       ├── handler/MqttClientHandler.java  # 单一 Netty handler
│       ├── AckTracker.java              # packetId → PendingOutbound
│       ├── InboundQos.java              # QoS1 PUBACK / QoS2 PUBREC→PUBCOMP (受背压门控)
│       ├── MqttInbox.java                # 入站 Flux + 双向背压
│       ├── MqttOutbox.java               # 出站 inflight 信号量
│       ├── SubscriptionStore.java        # topicFilter → Sinks.Many (重连恢复)
│       ├── MqttMessageService.java       # 协议适配接口
│       ├── reconnect/MqttAutoReconnect.java
│       ├── buffer/MessageBuffer.java
│       ├── transport/TransportFactory.java  MqttSslConfig.java  MqttWebSocketConfig.java
│       │   └── ws/ByteBufToWebSocketFrameEncoder.java  WebSocketFrameToByteBufDecoder.java
│       └── util/PacketIdManager.java  NettyUtil.java  TopicMatcher.java
```

## 14. 测试策略

| 层 | 测试 | 工具 |
|---|---|---|
| 单元 | `PacketIdManager` 回绕/跳0、`MessageBuffer` 容量/字节上限/满拒绝、`AckTracker` complete、`TopicMatcher` 通配符（+/#） | JUnit5 |
| 编解码 | v3/v5 各消息 encode→decode 往返一致性；CONNECT wire 字节级断言 | `netty-codec-mqtt` EmbeddedChannel |
| 背压 | inflight 满时 `publish()` Mono 挂起而非报错；下游不 request 时 PUBACK 不发（EmbeddedChannel + StepVerifier 验证未 flush ACK） | reactor `StepVerifier` |
| 重连 | 模拟 transport 断开 → 指数退避调度 → 重连成功恢复订阅；用 `VirtualTimeScheduler` 验证 backoff 时序 | reactor test |
| 集成（TCP） | `Mqtt3ClientIT`（15）/ `Mqtt5ClientIT`（12）：connect/subscribe/publish/receive/disconnect；QoS0/1/2；Async/Blocking；retain/会话持久化；v5 Properties | 内嵌 `EmbeddedBrokerHolder`（随机 TCP 端口）或外部 broker（默认 TCP `1883`，`-Djmqx.it.broker.port` 可覆盖） |
| 集成（传输层） | `Mqtt3TransportIT` / `Mqtt5TransportIT`（各 3）：MQTTS(8883) / WS(1884) / WSS(8884) 连接 + QoS1 发布订阅 | 同上；内嵌 broker 四监听全开；外部 broker 默认 1883/8883/1884/8884 |
| 压力 | `Mqtt3ClientStressTest` / `Mqtt5ClientStressTest`：连接/发布/订阅独立场景 | **外部 broker**（不内嵌）；`-Djmqx.stress.tests=true` |
| v3/v5 互通 | client v3/v5 ↔ jmqx-broker | jmqx-broker |

**不使用 mock broker**——直接用真实的 jmqx-broker 做集成测试（项目已有该模块），最贴近生产。单元测试用 `EmbeddedChannel` 驱动 pipeline，无需真实网络。

## 15. 实现范围

### 第一版包含
- MQTT 3.1.1 完整客户端（`v3` 包，含 message/internal）
- MQTT 5 完整客户端（`v5` 包，含 Properties/Reason Code/Receive Maximum）
- TCP / SSL/TLS / WebSocket / WSS 传输
- QoS 0/1/2 完整实现（出站 + 入站）
- 三种 API（Async/Reactive/Blocking）
- 双向背压（入站 request 门控 ACK + 出站 inflight 信号量）
- 自动重连（指数退避 + jitter，reactor 异步调度）
- 断线缓存（flush 重注 ACK）
- Keepalive PINGREQ/PINGRESP
- 单一 PacketIdManager
- Builder 链式 API（`.useMqttVersion3()` / `.useMqttVersion5()`）
- 连接/断开生命周期监听器
- Session 恢复策略（cleanSession/sessionExpiry）

### 不含（YAGNI）
- SOCKS 代理
- 消息拦截器链
- 高级统计/指标
- 持久化断线缓存（内存队列已覆盖默认场景；持久化留 SPI 扩展点以后再加）

## 16. 依赖

```xml
<!-- 已有依赖 (jmqx-client/pom.xml) -->
reactor-netty              MqttRxClient 传输层 (TcpClient/HttpClient/Connection)
netty-codec-mqtt           MQTT 协议编解码 (含 v5 Properties 支持)
reactor-core               Reactive 核心 (Mono/Flux/Sinks)
lombok                     Bean 工具
slf4j-api                  日志
hutool-all                 工具类
junit-jupiter              测试

<!-- 无需新增依赖 (无 Dagger / 无 RxJava / 无 mock-broker) -->
<!-- 版本由 jmqx-parent 管理: reactor 2024.0.4, netty 4.1.119.Final -->
```

## 17. 已知旧设计缺陷与本版修复对照

| 旧设计缺陷 | 本版修复 |
|---|---|
| 用原生 Netty Bootstrap/NioEventLoopGroup，非 reactor-netty | 用 reactor-netty `TcpClient`/`Connection`，全程 `Mono`/`Flux` |
| `bootstrap.connect().sync()` 阻塞调用 | `connect()` 返回 `Mono<Connection>`，非阻塞 |
| `Thread.sleep()` 在 listener 回调（死锁源） | `Mono.delay()` reactor 异步调度 |
| v5 仅占位 | v5 完整实现（Properties/ReasonCode/ReceiveMaximum） |
| 背压未实现（`publishes()` 返回 `UnsupportedOperationException`） | 双向背压（request 门控 ACK + inflight 信号量） |
| 两个 PacketIdManager 导致 packetId 碰撞 | 单一 PacketIdManager |
| 断线缓存 flush 不重注 ACK | flush 走完整 `publish()` 路径，重注 AckTracker |
| 无 keepalive PINGREQ/PINGRESP | IdleStateHandler + PINGREQ/PINGRESP |
| `Mqtt3ChannelInitializer` 忽略 connAckSink 参数 | 用 `Sinks.One` 正确串联 CONNACK |
| 散落 3 个 ChannelDuplexHandler，wiring 混乱 | 单一 `MqttClientHandler`，内部委托普通 Java 对象 |
| `subscribe()` NPE 的 pipeline-context 表达式 | `SubscriptionStore` 由 client 注入 packetId，简洁路由 |
| `subscribePublishes`/`publishes`/`unsubscribe` 是 `UnsupportedOperationException` | 全部实现 |
