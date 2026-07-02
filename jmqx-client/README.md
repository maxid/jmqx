# jmqx-client — Reactor-Netty MQTT Client

基于 reactor-netty + netty-codec-mqtt 的高性能异步 MQTT 客户端，对标 [HiveMQ MQTT Client](https://github.com/hivemq/hivemq-mqtt-client) API 风格。

## 特性

- **MQTT 3.1.1 + 5.0** 双协议支持
- **三种 API**：Reactor (`Mono`/`Flux`)、`CompletableFuture`、阻塞
- **双向背压**：入站 `request(n)` 门控 ACK；出站 inflight 信号量
- **自动重连**：指数退避 + jitter（`Mono.delay`，无阻塞 sleep）
- **断线缓存**：离线期间缓冲 QoS1/2 出站消息，重连后自动 flush
- **传输**：TCP / TLS / WebSocket / WSS

## 快速开始（MQTT 3.1.1）

```java
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
        .topicFilters(List.of(
            Mqtt3TopicFilter.builder().topicFilter("sensor/#").qos(QoS.AT_LEAST_ONCE).build()))
        .build())
    .doOnNext(p -> System.out.println(p.getTopic() + ": " + new String(p.getPayloadAsBytes())))
    .subscribe();

client.publish(Mqtt3Publish.builder()
        .topic("sensor/temp").payload("21.5".getBytes()).qos(QoS.AT_LEAST_ONCE).build())
    .block();
```

## 快速开始（MQTT 5.0）

```java
Mqtt5RxClient client = MqttClient.builder().useMqttVersion5()
    .serverHost("localhost").serverPort(1883)
    .identifier("v5-client")
    .cleanStart(true)
    .sessionExpiryInterval(3600)
    .receiveMaximum(100)
    .automaticReconnect()
    .buildRx();

client.connect().block(Duration.ofSeconds(5));
```

## API 视图

| 协议 | Reactor | Async | Blocking |
|------|---------|-------|----------|
| v3 | `Mqtt3RxClient` | `Mqtt3AsyncClient` | `Mqtt3BlockingClient` |
| v5 | `Mqtt5RxClient` | `Mqtt5AsyncClient` | `Mqtt5BlockingClient` |

构建方式：

```java
MqttClient.builder().useMqttVersion3()  // 或 useMqttVersion5()
    .serverHost("localhost").serverPort(1883)
    .identifier("id")
    .buildRx();    // 或 buildAsync() / buildBlocking()
```

## 测试

```bash
# 单元测试（默认排除 *IT.java）
mvn -pl jmqx-client test

# 集成测试（需 broker 在 localhost:1883）
mvn -pl jmqx-client test -Dtest=Mqtt3ClientIT
mvn -pl jmqx-client test -Dtest=Mqtt5ClientIT
```

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
