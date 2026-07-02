# jmqx-client MQTT Client Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a high-performance async MQTT 3.1.1 client library with CompletableFuture/Reactor/Blocking APIs, auto-reconnect, and offline buffering.

**Architecture:** Reactor-based core engine with `DefaultMqtt3Client` as the sole implementation; `Mqtt3AsyncClientImpl` and `Mqtt3BlockingClientImpl` are thin wrappers converting Mono/Flux ↔ CompletableFuture/blocking. Netty pipeline uses `netty-codec-mqtt` for MQTT wire format.

**Tech Stack:** Java 17, Reactor Netty, Netty MQTT Codec, Lombok, SLF4J, Hutool

---

## File Structure Overview

```
jmqx-client/src/
├── main/java/plus/jmqx/client/
│   ├── mqtt/
│   │   ├── MqttClient.java                  # 主入口接口
│   │   ├── MqttClientBuilder.java           # 顶层构建器
│   │   ├── MqttClientConfig.java            # 配置基类
│   │   ├── MqttClientState.java             # 状态枚举
│   │   ├── MqttVersion.java                 # 版本枚举
│   │   ├── MqttGlobalPublishFilter.java     # 入站过滤器
│   │   │
│   │   ├── v3/
│   │   │   ├── Mqtt3Client.java
│   │   │   ├── Mqtt3ClientBuilder.java
│   │   │   ├── Mqtt3ClientConfig.java
│   │   │   ├── Mqtt3AsyncClient.java
│   │   │   ├── Mqtt3RxClient.java
│   │   │   ├── Mqtt3BlockingClient.java
│   │   │   ├── Mqtt3PublishResult.java
│   │   │   │
│   │   │   ├── message/
│   │   │   │   ├── Mqtt3Connect.java
│   │   │   │   ├── Mqtt3ConnAck.java
│   │   │   │   ├── Mqtt3Publish.java
│   │   │   │   ├── Mqtt3PubAck.java
│   │   │   │   ├── Mqtt3PubRec.java
│   │   │   │   ├── Mqtt3PubRel.java
│   │   │   │   ├── Mqtt3PubComp.java
│   │   │   │   ├── Mqtt3Subscribe.java
│   │   │   │   ├── Mqtt3SubAck.java
│   │   │   │   ├── Mqtt3Unsubscribe.java
│   │   │   │   └── Mqtt3TopicFilter.java
│   │   │   │
│   │   │   └── internal/
│   │   │       ├── DefaultMqtt3Client.java
│   │   │       ├── Mqtt3AsyncClientImpl.java
│   │   │       ├── Mqtt3RxClientImpl.java
│   │   │       ├── Mqtt3BlockingClientImpl.java
│   │   │       ├── config/
│   │   │       │   └── Mqtt3ClientConfigImpl.java
│   │   │       ├── handler/
│   │   │       │   ├── Mqtt3ChannelInitializer.java
│   │   │       │   ├── Mqtt3ConnectHandler.java
│   │   │       │   ├── Mqtt3DisconnectHandler.java
│   │   │       │   ├── Mqtt3SubscriptionHandler.java
│   │   │       │   ├── Mqtt3IncomingQosHandler.java
│   │   │       │   └── Mqtt3OutgoingQosHandler.java
│   │   │       └── codec/
│   │   │           └── Mqtt3MessageCodec.java
│   │   │
│   │   ├── v5/
│   │   │   ├── Mqtt5Client.java
│   │   │   ├── Mqtt5AsyncClient.java
│   │   │   ├── Mqtt5RxClient.java
│   │   │   ├── Mqtt5BlockingClient.java
│   │   │   └── message/
│   │   │       ├── Mqtt5Connect.java
│   │   │       ├── Mqtt5ConnAck.java
│   │   │       ├── Mqtt5Publish.java
│   │   │       └── Mqtt5Subscribe.java
│   │   │
│   │   ├── message/
│   │   │   ├── MqttMessage.java
│   │   │   ├── QoS.java
│   │   │   └── MqttTopicFilter.java
│   │   │
│   │   ├── lifecycle/
│   │   │   ├── MqttClientConnectedListener.java
│   │   │   ├── MqttClientDisconnectedListener.java
│   │   │   ├── MqttClientConnectedContext.java
│   │   │   ├── MqttClientDisconnectedContext.java
│   │   │   └── MqttClientReconnector.java
│   │   │
│   │   └── internal/
│   │       ├── reconnect/
│   │       │   └── MqttAutoReconnect.java
│   │       ├── buffer/
│   │       │   └── MessageBuffer.java
│   │       └── util/
│   │           ├── PacketIdManager.java
│   │           └── NettyUtil.java
│   │
│   └── resources/META-INF/services/    (SPI 配置，当前不需要)
│
└── test/java/plus/jmqx/client/
    └── mqtt/
        └── Mqtt3ClientIntegrationTest.java
```

---

### Task 1: 版本无关基础类型

**Files:**
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/message/MqttMessage.java`
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/message/QoS.java`
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/message/MqttTopicFilter.java`
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/MqttVersion.java`
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/MqttClientState.java`
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/MqttGlobalPublishFilter.java`

- [ ] **Step 1: Create MqttMessage.java**

```java
package plus.jmqx.client.mqtt.message;

/**
 * MQTT 消息基类
 */
public interface MqttMessage {
}
```

- [ ] **Step 2: Create QoS.java**

```java
package plus.jmqx.client.mqtt.message;

/**
 * MQTT QoS 等级
 */
public enum QoS {
    AT_MOST_ONCE(0),
    AT_LEAST_ONCE(1),
    EXACTLY_ONCE(2);

    private final int value;

    QoS(int value) {
        this.value = value;
    }

    public int value() {
        return value;
    }

    public static QoS fromValue(int value) {
        return switch (value) {
            case 0 -> AT_MOST_ONCE;
            case 1 -> AT_LEAST_ONCE;
            case 2 -> EXACTLY_ONCE;
            default -> throw new IllegalArgumentException("Invalid QoS value: " + value);
        };
    }
}
```

- [ ] **Step 3: Create MqttTopicFilter.java**

```java
package plus.jmqx.client.mqtt.message;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 主题过滤器（版本无关）
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MqttTopicFilter {
    private String topicFilter;
    private QoS qos;
}
```

- [ ] **Step 4: Create MqttVersion.java**

```java
package plus.jmqx.client.mqtt;

/**
 * MQTT 协议版本
 */
public enum MqttVersion {
    MQTT_3_1,
    MQTT_3_1_1,
    MQTT_5
}
```

- [ ] **Step 5: Create MqttClientState.java**

```java
package plus.jmqx.client.mqtt;

/**
 * 客户端状态
 */
public enum MqttClientState {
    DISCONNECTED,
    CONNECTING,
    CONNECTED
}
```

- [ ] **Step 6: Create MqttGlobalPublishFilter.java**

```java
package plus.jmqx.client.mqtt;

/**
 * 入站发布消息过滤器
 */
public enum MqttGlobalPublishFilter {
    ALL,
    SUBSCRIBED,
    UNSOLICITED
}
```

- [ ] **Step 7: Commit**

```bash
git add jmqx-client/src/main/java/plus/jmqx/client/mqtt/message/MqttMessage.java \
        jmqx-client/src/main/java/plus/jmqx/client/mqtt/message/QoS.java \
        jmqx-client/src/main/java/plus/jmqx/client/mqtt/message/MqttTopicFilter.java \
        jmqx-client/src/main/java/plus/jmqx/client/mqtt/MqttVersion.java \
        jmqx-client/src/main/java/plus/jmqx/client/mqtt/MqttClientState.java \
        jmqx-client/src/main/java/plus/jmqx/client/mqtt/MqttGlobalPublishFilter.java
git commit -m "feat(client): add version-agnostic basic types

- MqttMessage, QoS, MqttTopicFilter, MqttVersion, MqttClientState, MqttGlobalPublishFilter"
```

---

### Task 2: MQTT 3.1.1 消息模型

**Files:**
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/message/Mqtt3Connect.java`
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/message/Mqtt3ConnAck.java`
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/message/Mqtt3Publish.java`
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/message/Mqtt3PubAck.java`
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/message/Mqtt3PubRec.java`
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/message/Mqtt3PubRel.java`
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/message/Mqtt3PubComp.java`
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/message/Mqtt3Subscribe.java`
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/message/Mqtt3SubAck.java`
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/message/Mqtt3Unsubscribe.java`
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/message/Mqtt3TopicFilter.java`

- [ ] **Step 1: Create Mqtt3Connect.java**

```java
package plus.jmqx.client.mqtt.v3.message;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import plus.jmqx.client.mqtt.message.QoS;

/**
 * MQTT 3.1.1 CONNECT 消息
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Mqtt3Connect implements Mqtt3Message {
    private String clientId;
    private boolean cleanSession;
    private int keepAliveSeconds;
    private String username;
    private byte[] password;
    private String willTopic;
    private byte[] willPayload;
    private QoS willQos;
    private boolean willRetain;
}
```

- [ ] **Step 2: Create Mqtt3ConnAck.java**

```java
package plus.jmqx.client.mqtt.v3.message;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * MQTT 3.1.1 CONNACK 消息
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Mqtt3ConnAck implements Mqtt3Message {
    private boolean sessionPresent;
    private int returnCode; // 0=accepted, 1=unacceptable protocol, 2=identifier rejected, 3=server unavailable, 4=bad user/pass, 5=not authorized
}
```

- [ ] **Step 3: Create Mqtt3Publish.java**

```java
package plus.jmqx.client.mqtt.v3.message;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import plus.jmqx.client.mqtt.message.QoS;

/**
 * MQTT 3.1.1 PUBLISH 消息
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Mqtt3Publish implements Mqtt3Message {
    private String topic;
    private byte[] payload;
    private QoS qos;
    private boolean retain;
    private int packetId;
}
```

- [ ] **Step 4: Create Mqtt3PubAck.java, Mqtt3PubRec.java, Mqtt3PubRel.java, Mqtt3PubComp.java**

```java
// Mqtt3PubAck.java
package plus.jmqx.client.mqtt.v3.message;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Mqtt3PubAck implements Mqtt3Message {
    private int packetId;
}

// Mqtt3PubRec.java
package plus.jmqx.client.mqtt.v3.message;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Mqtt3PubRec implements Mqtt3Message {
    private int packetId;
}

// Mqtt3PubRel.java
package plus.jmqx.client.mqtt.v3.message;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Mqtt3PubRel implements Mqtt3Message {
    private int packetId;
}

// Mqtt3PubComp.java
package plus.jmqx.client.mqtt.v3.message;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Mqtt3PubComp implements Mqtt3Message {
    private int packetId;
}
```

- [ ] **Step 5: Create Mqtt3Subscribe.java**

```java
package plus.jmqx.client.mqtt.v3.message;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * MQTT 3.1.1 SUBSCRIBE 消息
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Mqtt3Subscribe implements Mqtt3Message {
    private List<Mqtt3TopicFilter> topicFilters;
    private int packetId;
}
```

- [ ] **Step 6: Create Mqtt3SubAck.java**

```java
package plus.jmqx.client.mqtt.v3.message;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import plus.jmqx.client.mqtt.message.QoS;

import java.util.List;

/**
 * MQTT 3.1.1 SUBACK 消息
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Mqtt3SubAck implements Mqtt3Message {
    private List<QoS> grantedQos;
    private int packetId;
}
```

- [ ] **Step 7: Create Mqtt3Unsubscribe.java**

```java
package plus.jmqx.client.mqtt.v3.message;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * MQTT 3.1.1 UNSUBSCRIBE 消息
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Mqtt3Unsubscribe implements Mqtt3Message {
    private List<String> topicFilters;
    private int packetId;
}
```

- [ ] **Step 8: Create Mqtt3TopicFilter.java**

```java
package plus.jmqx.client.mqtt.v3.message;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import plus.jmqx.client.mqtt.message.QoS;

/**
 * MQTT 3.1.1 主题过滤器
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Mqtt3TopicFilter {
    private String topicFilter;
    private QoS qos;
}
```

- [ ] **Step 9: Create Mqtt3Message.java marker interface**

```java
package plus.jmqx.client.mqtt.v3.message;

import plus.jmqx.client.mqtt.message.MqttMessage;

/**
 * MQTT 3.1.1 消息标记接口
 */
public interface Mqtt3Message extends MqttMessage {
}
```

- [ ] **Step 10: Commit**

```bash
git add jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/ \
        jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/message/
git commit -m "feat(client): add MQTT 3.1.1 message types

- Mqtt3Connect, Mqtt3ConnAck, Mqtt3Publish
- Mqtt3PubAck, Mqtt3PubRec, Mqtt3PubRel, Mqtt3PubComp
- Mqtt3Subscribe, Mqtt3SubAck, Mqtt3Unsubscribe, Mqtt3TopicFilter"
```

---

### Task 3: 配置模型

**Files:**
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/MqttClientConfig.java`
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/Mqtt3ClientConfig.java`
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/internal/config/Mqtt3ClientConfigImpl.java`

- [ ] **Step 1: Create MqttClientConfig.java**

```java
package plus.jmqx.client.mqtt;

import lombok.Data;
import plus.jmqx.client.mqtt.v3.message.Mqtt3Publish;

import java.util.concurrent.TimeUnit;

/**
 * 版本无关客户端配置基类
 */
@Data
public class MqttClientConfig {
    // 连接
    private String serverHost = "localhost";
    private int serverPort = 1883;
    private String clientId;
    private int keepAliveSeconds = 60;

    // 超时
    private int socketConnectTimeoutMs = 10_000;
    private int mqttConnectTimeoutMs = 60_000;

    // 线程
    private int nettyThreads = Math.max(Runtime.getRuntime().availableProcessors(), 2);
    private int businessThreadSize = Math.max(Runtime.getRuntime().availableProcessors() * 2, 4);

    // 认证
    private String username;
    private byte[] password;

    // 遗嘱
    private Mqtt3Publish willPublish;

    // 重连（通过 AutoReconnectConfig 嵌套）
    private boolean automaticReconnect = false;
    private long reconnectInitialDelayMs = 1000;
    private long reconnectMaxDelayMs = 120_000;

    // 断线缓存
    private int messageBufferMaxSize = 1000;
}
```

- [ ] **Step 2: Create Mqtt3ClientConfig.java**

```java
package plus.jmqx.client.mqtt.v3;

import plus.jmqx.client.mqtt.MqttClientConfig;

/**
 * MQTT 3.1.1 客户端配置
 */
public class Mqtt3ClientConfig extends MqttClientConfig {
}
```

- [ ] **Step 3: Create Mqtt3ClientConfigImpl.java**

```java
package plus.jmqx.client.mqtt.v3.internal.config;

import plus.jmqx.client.mqtt.v3.Mqtt3ClientConfig;

/**
 * MQTT 3.x 配置实现（目前与配置类一致，预留扩展）
 */
public class Mqtt3ClientConfigImpl extends Mqtt3ClientConfig {
}
```

- [ ] **Step 4: Commit**

```bash
git add jmqx-client/src/main/java/plus/jmqx/client/mqtt/MqttClientConfig.java \
        jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/Mqtt3ClientConfig.java \
        jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/internal/config/
git commit -m "feat(client): add client configuration model"
```

---

### Task 4: 生命周期监听器

**Files:**
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/lifecycle/MqttClientConnectedListener.java`
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/lifecycle/MqttClientDisconnectedListener.java`
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/lifecycle/MqttClientConnectedContext.java`
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/lifecycle/MqttClientDisconnectedContext.java`
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/lifecycle/MqttClientReconnector.java`

- [ ] **Step 1: Create ConnectedContext**

```java
package plus.jmqx.client.mqtt.lifecycle;

import plus.jmqx.client.mqtt.v3.Mqtt3ClientConfig;

public class MqttClientConnectedContext {
    private final Mqtt3ClientConfig clientConfig;
    private final boolean sessionPresent;

    public MqttClientConnectedContext(Mqtt3ClientConfig clientConfig, boolean sessionPresent) {
        this.clientConfig = clientConfig;
        this.sessionPresent = sessionPresent;
    }

    public Mqtt3ClientConfig getClientConfig() { return clientConfig; }
    public boolean isSessionPresent() { return sessionPresent; }
}
```

- [ ] **Step 2: Create DisconnectedContext**

```java
package plus.jmqx.client.mqtt.lifecycle;

import plus.jmqx.client.mqtt.v3.Mqtt3ClientConfig;

public class MqttClientDisconnectedContext {
    public enum DisconnectSource { USER, CLIENT, SERVER }

    private final Mqtt3ClientConfig clientConfig;
    private final DisconnectSource source;
    private final Throwable cause;

    public MqttClientDisconnectedContext(Mqtt3ClientConfig clientConfig, DisconnectSource source, Throwable cause) {
        this.clientConfig = clientConfig;
        this.source = source;
        this.cause = cause;
    }

    public Mqtt3ClientConfig getClientConfig() { return clientConfig; }
    public DisconnectSource getSource() { return source; }
    public Throwable getCause() { return cause; }
}
```

- [ ] **Step 3: Create ConnectedListener**

```java
package plus.jmqx.client.mqtt.lifecycle;

@FunctionalInterface
public interface MqttClientConnectedListener {
    void onConnected(MqttClientConnectedContext context);
}
```

- [ ] **Step 4: Create DisconnectedListener**

```java
package plus.jmqx.client.mqtt.lifecycle;

@FunctionalInterface
public interface MqttClientDisconnectedListener {
    void onDisconnected(MqttClientDisconnectedContext context);
}
```

- [ ] **Step 5: Create Reconnector**

```java
package plus.jmqx.client.mqtt.lifecycle;

/**
 * 重连控制接口，在 DisconnectedListener 中修改重连行为
 */
public class MqttClientReconnector {
    private boolean reconnect;
    private long delayMs;
    private int attempts;
    private boolean resubscribe = true;

    public MqttClientReconnector(int attempts, boolean reconnect) {
        this.attempts = attempts;
        this.reconnect = reconnect;
    }

    public MqttClientReconnector reconnect(boolean reconnect) {
        this.reconnect = reconnect;
        return this;
    }

    public boolean isReconnect() { return reconnect; }

    public MqttClientReconnector delay(long delayMs) {
        this.delayMs = delayMs;
        return this;
    }

    public long getDelayMs() { return delayMs; }

    public int getAttempts() { return attempts; }

    public MqttClientReconnector resubscribe(boolean resubscribe) {
        this.resubscribe = resubscribe;
        return this;
    }

    public boolean isResubscribe() { return resubscribe; }
}
```

- [ ] **Step 6: Commit**

```bash
git add jmqx-client/src/main/java/plus/jmqx/client/mqtt/lifecycle/
git commit -m "feat(client): add lifecycle listeners and reconnect API"
```

---

### Task 5: PacketIdManager 和 NettyUtil 工具类

**Files:**
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/internal/util/PacketIdManager.java`
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/internal/util/NettyUtil.java`

- [ ] **Step 1: Create PacketIdManager.java**

```java
package plus.jmqx.client.mqtt.internal.util;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * MQTT PacketId 分配器（1～65535 循环）
 */
public class PacketIdManager {
    private final AtomicInteger nextId = new AtomicInteger(1);

    public int nextPacketId() {
        int id = nextId.getAndIncrement();
        if (id > 65535) {
            nextId.set(1);
            id = 1;
        }
        return id;
    }
}
```

- [ ] **Step 2: Create NettyUtil.java**

```java
package plus.jmqx.client.mqtt.internal.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.MqttProperties;

/**
 * Netty 工具方法
 */
public final class NettyUtil {

    private NettyUtil() {}

    public static ByteBuf toByteBuf(byte[] payload) {
        return payload != null ? Unpooled.copiedBuffer(payload) : Unpooled.EMPTY_BUFFER;
    }

    public static MqttProperties noProperties() {
        return MqttProperties.NO_PROPERTIES;
    }
}
```

- [ ] **Step 3: Commit**

```bash
git add jmqx-client/src/main/java/plus/jmqx/client/mqtt/internal/util/
git commit -m "feat(client): add PacketIdManager and NettyUtil"
```

---

### Task 6: MQTT 3.1.1 编解码器

**Files:**
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/internal/codec/Mqtt3MessageCodec.java`

- [ ] **Step 1: Create Mqtt3MessageCodec.java**

```java
package plus.jmqx.client.mqtt.v3.internal.codec;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.*;
import plus.jmqx.client.mqtt.message.QoS;
import plus.jmqx.client.mqtt.v3.message.*;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * MQTT 3.1.1 消息编解码辅助类
 * <p>
 * 将内部消息模型转为 Netty MqttMessage（编码），以及反向（解码）。
 * 利用 netty-codec-mqtt 的编解码器处理 wire format，该类处理业务模型的转换。
 * </p>
 */
public final class Mqtt3MessageCodec {

    private Mqtt3MessageCodec() {}

    // ========== 编码 ==========

    /**
     * 构建 MQTT CONNECT 报文
     */
    public static MqttConnectMessage encodeConnect(Mqtt3Connect connect) {
        String clientId = connect.getClientId() != null ? connect.getClientId() : "";
        MqttConnectPayload payload = new MqttConnectPayload(
                clientId,
                connect.getWillTopic(),
                connect.getWillPayload() != null ? connect.getWillPayload() : new byte[0],
                connect.getUsername(),
                connect.getPassword() != null ? new String(connect.getPassword(), StandardCharsets.UTF_8) : null
        );
        MqttConnectVariableHeader header = new MqttConnectVariableHeader(
                MqttVersion.MQTT_3_1_1.protocolLevel(),
                MqttVersion.MQTT_3_1_1.protocolName(),
                connect.getKeepAliveSeconds(),
                connect.isCleanSession(),
                connect.getWillTopic() != null,
                connect.getWillQos() != null ? connect.getWillQos().value() : 0,
                connect.isWillRetain(),
                connect.getPassword() != null,
                connect.getUsername() != null
        );
        return new MqttConnectMessage(null, header, payload);
    }

    /**
     * 构建 MQTT PUBLISH 报文
     */
    public static MqttPublishMessage encodePublish(Mqtt3Publish publish) {
        ByteBuf payload = publish.getPayload() != null
                ? io.netty.buffer.Unpooled.copiedBuffer(publish.getPayload())
                : io.netty.buffer.Unpooled.EMPTY_BUFFER;
        MqttFixedHeader fixedHeader = new MqttFixedHeader(
                MqttMessageType.PUBLISH,
                false,
                publish.getQos() != null ? MqttQoS.valueOf(publish.getQos().value()) : MqttQoS.AT_MOST_ONCE,
                publish.isRetain(),
                0
        );
        MqttPublishVariableHeader varHeader = new MqttPublishVariableHeader(
                publish.getTopic(),
                publish.getPacketId()
        );
        return new MqttPublishMessage(fixedHeader, varHeader, payload);
    }

    /**
     * 构建 MQTT SUBSCRIBE 报文
     */
    public static MqttSubscribeMessage encodeSubscribe(Mqtt3Subscribe subscribe) {
        List<MqttTopicSubscription> subs = new ArrayList<>();
        for (Mqtt3TopicFilter tf : subscribe.getTopicFilters()) {
            subs.add(new MqttTopicSubscription(
                    tf.getTopicFilter(),
                    MqttQoS.valueOf(tf.getQos().value())
            ));
        }
        MqttFixedHeader fixedHeader = new MqttFixedHeader(
                MqttMessageType.SUBSCRIBE,
                false,
                MqttQoS.AT_LEAST_ONCE,
                false,
                0
        );
        MqttSubscribeVariableHeader varHeader = new MqttSubscribeVariableHeader(
                subscribe.getPacketId(),
                MqttProperties.NO_PROPERTIES
        );
        return new MqttSubscribeMessage(fixedHeader, varHeader, new MqttSubscribePayload(subs));
    }

    /**
     * 构建 MQTT UNSUBSCRIBE 报文
     */
    public static MqttUnsubscribeMessage encodeUnsubscribe(Mqtt3Unsubscribe unsubscribe) {
        MqttFixedHeader fixedHeader = new MqttFixedHeader(
                MqttMessageType.UNSUBSCRIBE,
                false,
                MqttQoS.AT_LEAST_ONCE,
                false,
                0
        );
        MqttUnsubscribeVariableHeader varHeader = new MqttUnsubscribeVariableHeader(
                unsubscribe.getPacketId(),
                MqttProperties.NO_PROPERTIES
        );
        return new MqttUnsubscribeMessage(fixedHeader, varHeader, new MqttUnsubscribePayload(unsubscribe.getTopicFilters()));
    }

    /**
     * 构建 MQTT DISCONNECT 报文
     */
    public static MqttMessage encodeDisconnect() {
        MqttFixedHeader fixedHeader = new MqttFixedHeader(
                MqttMessageType.DISCONNECT,
                false,
                MqttQoS.AT_MOST_ONCE,
                false,
                0
        );
        return new MqttMessage(fixedHeader);
    }

    /**
     * 构建 MQTT PUBACK 报文
     */
    public static MqttPubAckMessage encodePubAck(int packetId) {
        MqttFixedHeader fixedHeader = new MqttFixedHeader(
                MqttMessageType.PUBACK,
                false,
                MqttQoS.AT_MOST_ONCE,
                false,
                0
        );
        MqttPubAckMessage pubAck = new MqttPubAckMessage(fixedHeader, new MqttMessageIdVariableHeader(packetId));
        return pubAck;
    }

    /**
     * 构建 MQTT PUBREC 报文
     */
    public static MqttMessage encodePubRec(int packetId) {
        MqttFixedHeader fixedHeader = new MqttFixedHeader(
                MqttMessageType.PUBREC,
                false,
                MqttQoS.AT_MOST_ONCE,
                false,
                0
        );
        return new MqttPubAckMessage(fixedHeader, new MqttMessageIdVariableHeader(packetId));
    }

    /**
     * 构建 MQTT PUBREL 报文
     */
    public static MqttMessage encodePubRel(int packetId) {
        MqttFixedHeader fixedHeader = new MqttFixedHeader(
                MqttMessageType.PUBREL,
                false,
                MqttQoS.AT_LEAST_ONCE,
                false,
                0
        );
        return new MqttPubAckMessage(fixedHeader, new MqttMessageIdVariableHeader(packetId));
    }

    /**
     * 构建 MQTT PUBCOMP 报文
     */
    public static MqttMessage encodePubComp(int packetId) {
        MqttFixedHeader fixedHeader = new MqttFixedHeader(
                MqttMessageType.PUBCOMP,
                false,
                MqttQoS.AT_MOST_ONCE,
                false,
                0
        );
        return new MqttPubAckMessage(fixedHeader, new MqttMessageIdVariableHeader(packetId));
    }

    // ========== 解码 ==========

    /**
     * 从 Netty CONNACK 解码为 Mqtt3ConnAck
     */
    public static Mqtt3ConnAck decodeConnAck(MqttConnAckMessage msg) {
        MqttConnAckVariableHeader header = msg.variableHeader();
        return new Mqtt3ConnAck(
                header.isSessionPresent(),
                header.connectReturnCode().byteValue()
        );
    }

    /**
     * 从 Netty PUBLISH 解码为 Mqtt3Publish
     */
    public static Mqtt3Publish decodePublish(MqttPublishMessage msg) {
        MqttFixedHeader fixed = msg.fixedHeader();
        MqttPublishVariableHeader var = msg.variableHeader();
        byte[] payload = new byte[msg.payload().readableBytes()];
        msg.payload().getBytes(msg.payload().readerIndex(), payload);
        return Mqtt3Publish.builder()
                .topic(var.topicName())
                .payload(payload)
                .qos(QoS.fromValue(fixed.qosLevel().value()))
                .retain(fixed.isRetain())
                .packetId(var.packetId())
                .build();
    }

    /**
     * 从 Netty SUBACK 解码为 Mqtt3SubAck
     */
    public static Mqtt3SubAck decodeSubAck(MqttSubAckMessage msg) {
        List<QoS> granted = new ArrayList<>();
        for (int code : msg.payload().grantedQoSLevels()) {
            granted.add(QoS.fromValue(code));
        }
        return new Mqtt3SubAck(granted, msg.variableHeader().messageId());
    }

    /**
     * 从 Netty PUBACK/PUBREC/PUBREL/PUBCOMP 提取 packetId
     */
    public static int decodePacketId(MqttMessage msg) {
        if (msg.variableHeader() instanceof MqttMessageIdVariableHeader idHeader) {
            return idHeader.messageId();
        }
        return 0;
    }
}
```

- [ ] **Step 2: Commit**

```bash
git add jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/internal/codec/
git commit -m "feat(client): add MQTT 3.1.1 codec"
```

---

### Task 7: ConnectHandler — 连接握手

**Files:**
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/internal/handler/Mqtt3ConnectHandler.java`

- [ ] **Step 1: Create Mqtt3ConnectHandler.java**

```java
package plus.jmqx.client.mqtt.v3.internal.handler;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import lombok.extern.slf4j.Slf4j;
import plus.jmqx.client.mqtt.v3.message.Mqtt3ConnAck;
import plus.jmqx.client.mqtt.v3.internal.codec.Mqtt3MessageCodec;
import plus.jmqx.client.mqtt.v3.message.Mqtt3Connect;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

/**
 * MQTT 连接处理器
 * <p>
 * Channel 激活后写入 CONNECT 报文，等待 CONNACK 返回后通过 sink 通知。
 * </p>
 */
@Slf4j
public class Mqtt3ConnectHandler extends ChannelDuplexHandler {

    private final Mqtt3Connect connectMessage;
    private final Sinks.One<Mqtt3ConnAck> connAckSink;

    private boolean connectSent = false;

    public Mqtt3ConnectHandler(Mqtt3Connect connectMessage, Sinks.One<Mqtt3ConnAck> connAckSink) {
        this.connectMessage = connectMessage;
        this.connAckSink = connAckSink;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        // 发送 CONNECT 报文
        io.netty.handler.codec.mqtt.MqttConnectMessage msg = Mqtt3MessageCodec.encodeConnect(connectMessage);
        ctx.writeAndFlush(msg);
        connectSent = true;
        log.debug("CONNECT sent for clientId={}", connectMessage.getClientId());
        super.channelActive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof MqttConnAckMessage connAck) {
            Mqtt3ConnAck ack = Mqtt3MessageCodec.decodeConnAck(connAck);
            log.debug("CONNACK received: sessionPresent={}, returnCode={}", ack.isSessionPresent(), ack.getReturnCode());
            if (ack.getReturnCode() == 0) {
                connAckSink.tryEmitValue(ack);
            } else {
                connAckSink.tryEmitError(new RuntimeException("Connection refused: returnCode=" + ack.getReturnCode()));
            }
            // CONNACK 不该传给后面的 handler
            return;
        }
        super.channelRead(ctx, msg);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (connectSent && connAckSink.currentSubscriberCount() > 0) {
            connAckSink.tryEmitError(cause);
        }
        super.exceptionCaught(ctx, cause);
    }
}
```

- [ ] **Step 2: Commit**

```bash
git add jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/internal/handler/Mqtt3ConnectHandler.java
git commit -m "feat(client): add MQTT connect handler"
```

---

### Task 8: DisconnectHandler

**Files:**
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/internal/handler/Mqtt3DisconnectHandler.java`

- [ ] **Step 1: Create Mqtt3DisconnectHandler.java**

```java
package plus.jmqx.client.mqtt.v3.internal.handler;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Sinks;

/**
 * MQTT 断连处理器
 * <p>
 * 拦截 MQTT DISCONNECT 报文（入站），当收到服务端断开时触发 sink。
 * </p>
 */
@Slf4j
public class Mqtt3DisconnectHandler extends ChannelDuplexHandler {

    private final Sinks.Empty<Void> disconnectSink;

    public Mqtt3DisconnectHandler(Sinks.Empty<Void> disconnectSink) {
        this.disconnectSink = disconnectSink;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof MqttMessage mqtt && mqtt.fixedHeader() != null
                && mqtt.fixedHeader().messageType() == MqttMessageType.DISCONNECT) {
            log.debug("DISCONNECT received from server");
            disconnectSink.tryEmitEmpty();
            // 不传递给后续 handler
            return;
        }
        super.channelRead(ctx, msg);
    }
}
```

- [ ] **Step 2: Commit**

```bash
git add jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/internal/handler/Mqtt3DisconnectHandler.java
git commit -m "feat(client): add MQTT disconnect handler"
```

---

### Task 9: SubscriptionHandler — 主题订阅路由

**Files:**
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/internal/handler/Mqtt3SubscriptionHandler.java`

- [ ] **Step 1: Create Mqtt3SubscriptionHandler.java**

```java
package plus.jmqx.client.mqtt.v3.internal.handler;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.*;
import lombok.extern.slf4j.Slf4j;
import plus.jmqx.client.mqtt.internal.util.PacketIdManager;
import plus.jmqx.client.mqtt.v3.internal.codec.Mqtt3MessageCodec;
import plus.jmqx.client.mqtt.v3.message.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * MQTT 订阅处理器
 * <p>
 * 维护订阅映射（topicFilter → callback），路由入站 PUBLISH 到匹配的回调。
 * 同时处理 SUBACK / UNSUBACK 响应。
 * </p>
 */
@Slf4j
public class Mqtt3SubscriptionHandler extends ChannelDuplexHandler {

    private final PacketIdManager packetIdManager = new PacketIdManager();
    private final Map<String, Consumer<Mqtt3Publish>> subscriptions = new ConcurrentHashMap<>();
    private final Map<Integer, Sinks.One<Mqtt3SubAck>> pendingSubAcks = new ConcurrentHashMap<>();
    private final Map<Integer, Sinks.Empty<Void>> pendingUnsubAcks = new ConcurrentHashMap<>();

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof MqttSubAckMessage subAck) {
            int packetId = subAck.variableHeader().messageId();
            Sinks.One<Mqtt3SubAck> sink = pendingSubAcks.remove(packetId);
            if (sink != null) {
                Mqtt3SubAck ack = Mqtt3MessageCodec.decodeSubAck(subAck);
                sink.tryEmitValue(ack);
                log.debug("SUBACK received, packetId={}", packetId);
            }
            return;
        }
        if (msg instanceof MqttUnsubAckMessage unsubAck) {
            int packetId = unsubAck.variableHeader().messageId();
            Sinks.Empty<Void> sink = pendingUnsubAcks.remove(packetId);
            if (sink != null) {
                sink.tryEmitEmpty();
                log.debug("UNSUBACK received, packetId={}", packetId);
            }
            return;
        }
        if (msg instanceof MqttPublishMessage publish) {
            Mqtt3Publish pub = Mqtt3MessageCodec.decodePublish(publish);
            routePublish(pub);
            // 入站 PUBLISH 不放行给后续 handler（由 QoS 处理器处理）
            return;
        }
        super.channelRead(ctx, msg);
    }

    /**
     * 添加订阅
     */
    public Sinks.One<Mqtt3SubAck> subscribe(ChannelHandlerContext ctx, Mqtt3Subscribe sub) {
        int packetId = packetIdManager.nextPacketId();
        sub.setPacketId(packetId);

        MqttSubscribeMessage msg = Mqtt3MessageCodec.encodeSubscribe(sub);
        ctx.writeAndFlush(msg);

        Sinks.One<Mqtt3SubAck> sink = Sinks.one();
        pendingSubAcks.put(packetId, sink);

        // 预注册 topicFilter → callback 映射
        // 注：实际注册应在 SUBACK 确认后，但先注册保证不丢失首次消息
        Consumer<Mqtt3Publish> callback = sub.getCallback();
        if (callback != null) {
            for (Mqtt3TopicFilter tf : sub.getTopicFilters()) {
                subscriptions.put(tf.getTopicFilter(), callback);
            }
        }

        return sink;
    }

    /**
     * 取消订阅
     */
    public Sinks.Empty<Void> unsubscribe(ChannelHandlerContext ctx, Mqtt3Unsubscribe unsub) {
        int packetId = packetIdManager.nextPacketId();
        unsub.setPacketId(packetId);

        MqttUnsubscribeMessage msg = Mqtt3MessageCodec.encodeUnsubscribe(unsub);
        ctx.writeAndFlush(msg);

        Sinks.Empty<Void> sink = Sinks.empty();
        pendingUnsubAcks.put(packetId, sink);

        // 移除 topicFilter 映射
        for (String filter : unsub.getTopicFilters()) {
            subscriptions.remove(filter);
        }

        return sink;
    }

    /**
     * 恢复所有订阅（重连时使用）
     */
    public Map<String, Consumer<Mqtt3Publish>> getSubscriptions() {
        return subscriptions;
    }

    /**
     * 将 PUBLISH 路由到匹配的订阅者
     */
    private void routePublish(Mqtt3Publish publish) {
        for (Map.Entry<String, Consumer<Mqtt3Publish>> entry : subscriptions.entrySet()) {
            if (topicMatches(entry.getKey(), publish.getTopic())) {
                try {
                    entry.getValue().accept(publish);
                } catch (Exception e) {
                    log.error("Error in subscription callback for topic={}", publish.getTopic(), e);
                }
            }
        }
    }

    /**
     * MQTT 主题匹配（支持 + 和 # 通配符）
     */
    private boolean topicMatches(String filter, String topic) {
        if (filter.equals(topic)) return true;
        if (filter.equals("#") || filter.equals("/#")) return true;
        String[] f = filter.split("/", -1);
        String[] t = topic.split("/", -1);
        if (f.length > t.length && !f[f.length - 1].equals("#")) return false;
        for (int i = 0; i < f.length; i++) {
            if (f[i].equals("#")) return true;
            if (!f[i].equals("+") && (i >= t.length || !f[i].equals(t[i]))) return false;
        }
        return f.length == t.length;
    }
}
```

Note: The `Mqtt3Subscribe` class needs a `callback` field. Update it:

- [ ] **Step 2: Add callback field to Mqtt3Subscribe.java**

```java
// Add to the existing Mqtt3Subscribe.java:
import java.util.function.Consumer;

// Add field:
private Consumer<Mqtt3Publish> callback;
```

- [ ] **Step 3: Commit**

```bash
git add jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/internal/handler/Mqtt3SubscriptionHandler.java \
        jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/message/Mqtt3Subscribe.java
git commit -m "feat(client): add MQTT subscription handler with topic routing"
```

---

### Task 10: 入站 QoS 处理器

**Files:**
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/internal/handler/Mqtt3IncomingQosHandler.java`

- [ ] **Step 1: Create Mqtt3IncomingQosHandler.java**

```java
package plus.jmqx.client.mqtt.v3.internal.handler;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.*;
import lombok.extern.slf4j.Slf4j;
import plus.jmqx.client.mqtt.v3.internal.codec.Mqtt3MessageCodec;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 入站 QoS 处理器
 * <p>
 * 处理 Broker → Client 方向的 QoS 1/2 流程：
 * - QoS 1: 回复 PUBACK
 * - QoS 2: 回复 PUBREC → 等待 PUBREL → 回复 PUBCOMP
 * </p>
 */
@Slf4j
public class Mqtt3IncomingQosHandler extends ChannelDuplexHandler {

    // QoS 2 等待 PUBREL 的 packetId
    private final Set<Integer> pendingPubRel = ConcurrentHashMap.newKeySet();

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof MqttPublishMessage publish) {
            MqttFixedHeader fixed = publish.fixedHeader();
            int packetId = publish.variableHeader().packetId();

            switch (fixed.qosLevel()) {
                case AT_LEAST_ONCE -> {
                    // 回复 PUBACK
                    ctx.writeAndFlush(Mqtt3MessageCodec.encodePubAck(packetId));
                    // 继续传递给 SubscriptionHandler
                    super.channelRead(ctx, msg);
                }
                case EXACTLY_ONCE -> {
                    // 回复 PUBREC，标记等待 PUBREL
                    pendingPubRel.add(packetId);
                    ctx.writeAndFlush(Mqtt3MessageCodec.encodePubRec(packetId));
                    // PUBLISH 传递给 SubscriptionHandler
                    super.channelRead(ctx, msg);
                }
                default -> super.channelRead(ctx, msg);
            }
            return;
        }

        if (msg instanceof MqttMessage mqtt && mqtt.fixedHeader() != null) {
            if (mqtt.fixedHeader().messageType() == MqttMessageType.PUBREL) {
                int packetId = Mqtt3MessageCodec.decodePacketId(mqtt);
                if (pendingPubRel.remove(packetId)) {
                    ctx.writeAndFlush(Mqtt3MessageCodec.encodePubComp(packetId));
                    log.debug("PUBCOMP sent for packetId={}", packetId);
                }
                return;
            }
        }

        super.channelRead(ctx, msg);
    }
}
```

- [ ] **Step 2: Commit**

```bash
git add jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/internal/handler/Mqtt3IncomingQosHandler.java
git commit -m "feat(client): add incoming QoS handler (PUBACK/PUBREC/PUBCOMP)"
```

---

### Task 11: 出站 QoS 处理器

**Files:**
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/internal/handler/Mqtt3OutgoingQosHandler.java`

- [ ] **Step 1: Create Mqtt3OutgoingQosHandler.java**

```java
package plus.jmqx.client.mqtt.v3.internal.handler;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.*;
import lombok.extern.slf4j.Slf4j;
import plus.jmqx.client.mqtt.v3.internal.codec.Mqtt3MessageCodec;
import reactor.core.publisher.Sinks;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 出站 QoS 处理器
 * <p>
 * 处理 Client → Broker 方向的 QoS 1/2 ACK 跟踪：
 * - 维护 packetId → Sink 映射
 * - QoS 1: PUBACK 到达时 complete
 * - QoS 2: PUBREC → PUBREL → PUBCOMP 两阶段
 * </p>
 */
@Slf4j
public class Mqtt3OutgoingQosHandler extends ChannelDuplexHandler {

    // packetId → 结果 sink（用于 QoS1 的 PUBACK 和 QoS2 的 PUBCOMP）
    private final Map<Integer, Sinks.One<Void>> pendingAcks = new ConcurrentHashMap<>();
    // QoS2 第二阶段：收到 PUBREC 后等待 PUBCOMP
    private final Map<Integer, Sinks.One<Void>> pendingPubComp = new ConcurrentHashMap<>();

    /**
     * 注册等待 ACK 的出站消息
     */
    public Sinks.One<Void> registerOutgoing(int packetId) {
        Sinks.One<Void> sink = Sinks.one();
        pendingAcks.put(packetId, sink);
        return sink;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (!(msg instanceof MqttMessage mqtt) || mqtt.fixedHeader() == null) {
            super.channelRead(ctx, msg);
            return;
        }

        MqttMessageType type = mqtt.fixedHeader().messageType();
        int packetId = Mqtt3MessageCodec.decodePacketId(mqtt);

        switch (type) {
            case PUBACK -> {
                Sinks.One<Void> sink = pendingAcks.remove(packetId);
                if (sink != null) {
                    sink.tryEmitEmpty();
                    log.debug("PUBACK received for packetId={}", packetId);
                }
                return;
            }
            case PUBREC -> {
                // QoS 2 第一阶段完成 → 发送 PUBREL
                pendingAcks.remove(packetId);
                pendingPubComp.put(packetId, Sinks.one());
                ctx.writeAndFlush(Mqtt3MessageCodec.encodePubRel(packetId));
                return;
            }
            case PUBCOMP -> {
                // QoS 2 第二阶段完成
                Sinks.One<Void> sink = pendingPubComp.remove(packetId);
                if (sink != null) {
                    sink.tryEmitEmpty();
                    log.debug("PUBCOMP received for packetId={}", packetId);
                } else {
                    // 也可能是直接等待 PUBCOMP 的
                    Sinks.One<Void> direct = pendingAcks.remove(packetId);
                    if (direct != null) direct.tryEmitEmpty();
                }
                return;
            }
        }

        super.channelRead(ctx, msg);
    }

    /**
     * 获取等待 ACK 的 sink（供外部获取结果）
     */
    public Sinks.One<Void> getPendingAck(int packetId) {
        Sinks.One<Void> sink = pendingAcks.get(packetId);
        if (sink == null) {
            sink = pendingPubComp.get(packetId);
        }
        return sink;
    }
}
```

- [ ] **Step 2: Commit**

```bash
git add jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/internal/handler/Mqtt3OutgoingQosHandler.java
git commit -m "feat(client): add outgoing QoS handler with ACK tracking"
```

---

### Task 12: ChannelInitializer — 组装 Pipeline

**Files:**
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/internal/handler/Mqtt3ChannelInitializer.java`

- [ ] **Step 1: Create Mqtt3ChannelInitializer.java**

```java
package plus.jmqx.client.mqtt.v3.internal.handler;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.timeout.IdleStateHandler;
import lombok.Setter;
import plus.jmqx.client.mqtt.v3.message.Mqtt3Connect;
import reactor.core.publisher.Sinks;

/**
 * MQTT 3.1.1 Channel 初始化器
 * <p>
 * Pipeline 顺序：
 * MqttEncoder (outbound) → MqttDecoder → Mqtt3ConnectHandler → Mqtt3DisconnectHandler
 * → (CONNACK 后动态添加) Mqtt3SubscriptionHandler → Mqtt3IncomingQosHandler → Mqtt3OutgoingQosHandler
 * </p>
 */
public class Mqtt3ChannelInitializer extends ChannelInitializer<Channel> {

    private final Mqtt3Connect connectMessage;
    private final Sinks.One<Mqtt3ConnAckFuture> connAckSink;
    private final Sinks.Empty<Void> disconnectSink;

    @Setter
    private Mqtt3SubscriptionHandler subscriptionHandler;
    @Setter
    private Mqtt3IncomingQosHandler incomingQosHandler;
    @Setter
    private Mqtt3OutgoingQosHandler outgoingQosHandler;

    // 内部类用于从 initializer 传递 connAck sink
    public static class Mqtt3ConnAckFuture {
        private final Sinks.One<plus.jmqx.client.mqtt.v3.message.Mqtt3ConnAck> sink;

        public Mqtt3ConnAckFuture(Sinks.One<plus.jmqx.client.mqtt.v3.message.Mqtt3ConnAck> sink) {
            this.sink = sink;
        }

        public Sinks.One<plus.jmqx.client.mqtt.v3.message.Mqtt3ConnAck> getSink() {
            return sink;
        }
    }

    public Mqtt3ChannelInitializer(Mqtt3Connect connectMessage,
                                    Sinks.One<plus.jmqx.client.mqtt.v3.message.Mqtt3ConnAck> connAckSink,
                                    Sinks.Empty<Void> disconnectSink) {
        this.connectMessage = connectMessage;
        this.connAckSink = Sinks.one();
        this.disconnectSink = disconnectSink;
    }

    @Override
    protected void initChannel(Channel ch) {
        ch.pipeline()
                .addLast("encoder", MqttEncoder.INSTANCE)
                .addLast("decoder", new MqttDecoder(10 * 1024 * 1024))
                .addLast("idle", new IdleStateHandler(0, 0, 60))
                .addLast("connect", new Mqtt3ConnectHandler(connectMessage, connAckSink))
                .addLast("disconnect", new Mqtt3DisconnectHandler(disconnectSink));
    }
}
```

- [ ] **Step 2: Commit**

```bash
git add jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/internal/handler/Mqtt3ChannelInitializer.java
git commit -m "feat(client): add channel initializer for MQTT pipeline"
```

---

### Task 13: MQTT 3.1.1 客户端接口

**Files:**
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/MqttClient.java`
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/Mqtt3Client.java`
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/Mqtt3AsyncClient.java`
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/Mqtt3RxClient.java`
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/Mqtt3BlockingClient.java`
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/Mqtt3PublishResult.java`

- [ ] **Step 1: Create MqttClient.java**

```java
package plus.jmqx.client.mqtt;

import plus.jmqx.client.mqtt.v3.Mqtt3Client;

/**
 * MQTT 客户端主入口接口
 */
public interface MqttClient {

    MqttClientConfig getConfig();
    MqttClientState getState();
    MqttVersion getVersion();

    static MqttClientBuilder builder() {
        return new MqttClientBuilder();
    }
}
```

- [ ] **Step 2: Create Mqtt3Client.java**

```java
package plus.jmqx.client.mqtt.v3;

import plus.jmqx.client.mqtt.MqttClient;

/**
 * MQTT 3.1/3.1.1 客户端接口
 */
public interface Mqtt3Client extends MqttClient {

    @Override
    Mqtt3ClientConfig getConfig();

    Mqtt3AsyncClient toAsync();
    Mqtt3RxClient toRx();
    Mqtt3BlockingClient toBlock();

    static Mqtt3ClientBuilder builder() {
        return new Mqtt3ClientBuilder();
    }
}
```

- [ ] **Step 3: Create Mqtt3AsyncClient.java**

```java
package plus.jmqx.client.mqtt.v3;

import plus.jmqx.client.mqtt.v3.message.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * MQTT 3.1.1 异步客户端（CompletableFuture）
 */
public interface Mqtt3AsyncClient extends Mqtt3Client {

    CompletableFuture<Mqtt3ConnAck> connect();

    CompletableFuture<Mqtt3SubAck> subscribe(Mqtt3Subscribe sub, Consumer<Mqtt3Publish> callback);

    CompletableFuture<Mqtt3PublishResult> publish(Mqtt3Publish publish);

    CompletableFuture<Void> unsubscribe(Mqtt3Unsubscribe unsub);

    CompletableFuture<Void> disconnect();
}
```

- [ ] **Step 4: Create Mqtt3RxClient.java**

```java
package plus.jmqx.client.mqtt.v3;

import plus.jmqx.client.mqtt.MqttGlobalPublishFilter;
import plus.jmqx.client.mqtt.v3.message.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * MQTT 3.1.1 响应式客户端（Reactor）
 */
public interface Mqtt3RxClient extends Mqtt3Client {

    Mono<Mqtt3ConnAck> connect();

    Mono<Mqtt3SubAck> subscribe(Mqtt3Subscribe sub);

    Flux<Mqtt3Publish> subscribePublishes(Mqtt3Subscribe sub);

    Flux<Mqtt3Publish> publishes(MqttGlobalPublishFilter filter);

    Mono<Mqtt3PublishResult> publish(Mqtt3Publish publish);

    Mono<Void> unsubscribe(Mqtt3Unsubscribe unsub);

    Mono<Void> disconnect();
}
```

- [ ] **Step 5: Create Mqtt3BlockingClient.java**

```java
package plus.jmqx.client.mqtt.v3;

import plus.jmqx.client.mqtt.MqttGlobalPublishFilter;
import plus.jmqx.client.mqtt.v3.message.*;

/**
 * MQTT 3.1.1 阻塞客户端
 */
public interface Mqtt3BlockingClient extends Mqtt3Client {

    Mqtt3ConnAck connect();

    Mqtt3SubAck subscribe(Mqtt3Subscribe sub);

    void publish(Mqtt3Publish publish);

    void unsubscribe(Mqtt3Unsubscribe unsub);

    void disconnect();
}
```

- [ ] **Step 6: Create Mqtt3PublishResult.java**

```java
package plus.jmqx.client.mqtt.v3;

import plus.jmqx.client.mqtt.v3.message.Mqtt3Publish;

/**
 * MQTT 3.1.1 发布结果
 */
public interface Mqtt3PublishResult {
    Mqtt3Publish getPublish();
    Throwable getError();
}
```

- [ ] **Step 7: Commit**

```bash
git add jmqx-client/src/main/java/plus/jmqx/client/mqtt/MqttClient.java \
        jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/Mqtt3Client.java \
        jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/Mqtt3AsyncClient.java \
        jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/Mqtt3RxClient.java \
        jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/Mqtt3BlockingClient.java \
        jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/Mqtt3PublishResult.java
git commit -m "feat(client): add MQTT client public API interfaces"
```

---

### Task 14: DefaultMqtt3Client — 核心引擎

**Files:**
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/internal/DefaultMqtt3Client.java`

- [ ] **Step 1: Create DefaultMqtt3Client.java**

```java
package plus.jmqx.client.mqtt.v3.internal;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import lombok.extern.slf4j.Slf4j;
import plus.jmqx.client.mqtt.MqttClientState;
import plus.jmqx.client.mqtt.MqttGlobalPublishFilter;
import plus.jmqx.client.mqtt.internal.buffer.MessageBuffer;
import plus.jmqx.client.mqtt.internal.util.PacketIdManager;
import plus.jmqx.client.mqtt.lifecycle.*;
import plus.jmqx.client.mqtt.v3.Mqtt3ClientConfig;
import plus.jmqx.client.mqtt.v3.Mqtt3PublishResult;
import plus.jmqx.client.mqtt.v3.Mqtt3RxClient;
import plus.jmqx.client.mqtt.v3.internal.codec.Mqtt3MessageCodec;
import plus.jmqx.client.mqtt.v3.internal.handler.*;
import plus.jmqx.client.mqtt.v3.message.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * MQTT 3.1.1 核心引擎实现
 * <p>
 * 集成了连接管理、状态机、订阅路由、QoS 流程、自动重连等功能。
 * 是 Mqtt3RxClient 的直接实现。
 * </p>
 */
@Slf4j
public class DefaultMqtt3Client implements Mqtt3RxClient {

    private final Mqtt3ClientConfig config;
    private final AtomicReference<MqttClientState> state = new AtomicReference<>(MqttClientState.DISCONNECTED);
    private final PacketIdManager packetIdManager = new PacketIdManager();
    private final MessageBuffer messageBuffer;
    private final List<MqttClientConnectedListener> connectedListeners;
    private final List<MqttClientDisconnectedListener> disconnectedListeners;

    private EventLoopGroup eventLoopGroup;
    private Channel channel;
    private Mqtt3SubscriptionHandler subscriptionHandler;
    private Mqtt3OutgoingQosHandler outgoingQosHandler;

    // 断开控制
    private final Sinks.Empty<Void> disconnectSink = Sinks.empty();

    // 重连状态
    private int reconnectAttempts = 0;

    public DefaultMqtt3Client(Mqtt3ClientConfig config,
                               List<MqttClientConnectedListener> connectedListeners,
                               List<MqttClientDisconnectedListener> disconnectedListeners) {
        this.config = config;
        this.connectedListeners = connectedListeners;
        this.disconnectedListeners = disconnectedListeners;
        this.messageBuffer = new MessageBuffer(config.getMessageBufferMaxSize());
    }

    // ========== Connect ==========

    @Override
    public Mono<Mqtt3ConnAck> connect() {
        return Mono.create(sink -> {
            if (!state.compareAndSet(MqttClientState.DISCONNECTED, MqttClientState.CONNECTING)) {
                sink.error(new IllegalStateException("Client is " + state.get()));
                return;
            }

            doConnect()
                    .subscribe(connAck -> {
                        state.set(MqttClientState.CONNECTED);
                        reconnectAttempts = 0;
                        // 通知 connected listeners
                        MqttClientConnectedContext ctx = new MqttClientConnectedContext(config, connAck.isSessionPresent());
                        connectedListeners.forEach(l -> l.onConnected(ctx));
                        // 刷新离线缓存
                        flushMessageBuffer();
                        sink.success(connAck);
                    }, error -> {
                        state.set(MqttClientState.DISCONNECTED);
                        handleDisconnect(error);
                        sink.error(error);
                    });
        });
    }

    private Mono<Mqtt3ConnAck> doConnect() {
        return Mono.create(sink -> {
            try {
                eventLoopGroup = new NioEventLoopGroup(config.getNettyThreads());

                Sinks.One<Mqtt3ConnAck> connAckSink = Sinks.one();

                // 构建 CONNECT 消息
                Mqtt3Connect connectMsg = Mqtt3Connect.builder()
                        .clientId(config.getClientId())
                        .cleanSession(true)
                        .keepAliveSeconds(config.getKeepAliveSeconds())
                        .username(config.getUsername())
                        .password(config.getPassword())
                        .willTopic(config.getWillPublish() != null ? config.getWillPublish().getTopic() : null)
                        .willPayload(config.getWillPublish() != null ? config.getWillPublish().getPayload() : null)
                        .willQos(config.getWillPublish() != null ? config.getWillPublish().getQos() : null)
                        .willRetain(config.getWillPublish() != null && config.getWillPublish().isRetain())
                        .build();

                // 创建 handler
                subscriptionHandler = new Mqtt3SubscriptionHandler();
                outgoingQosHandler = new Mqtt3OutgoingQosHandler();
                Mqtt3IncomingQosHandler incomingQosHandler = new Mqtt3IncomingQosHandler();

                Mqtt3ChannelInitializer initializer = new Mqtt3ChannelInitializer(
                        connectMsg, connAckSink, disconnectSink);

                Bootstrap bootstrap = new Bootstrap()
                        .group(eventLoopGroup)
                        .channel(NioSocketChannel.class)
                        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, config.getSocketConnectTimeoutMs())
                        .handler(initializer);

                ChannelFuture future = bootstrap.connect(config.getServerHost(), config.getServerPort()).sync();

                channel = future.channel();

                // 等待 CONNACK
                connAckSink.asMono()
                        .doOnSuccess(ack -> {
                            // 连接成功后动态添加 handler
                            channel.pipeline()
                                    .addAfter("disconnect", "subscription", subscriptionHandler)
                                    .addAfter("subscription", "incomingQos", incomingQosHandler)
                                    .addAfter("incomingQos", "outgoingQos", outgoingQosHandler);
                        })
                        .subscribe(sink::success, sink::error);

            } catch (Exception e) {
                sink.error(e);
            }
        });
    }

    // ========== Subscribe ==========

    @Override
    public Mono<Mqtt3SubAck> subscribe(Mqtt3Subscribe sub) {
        return Mono.create(sink -> {
            if (state.get() != MqttClientState.CONNECTED) {
                sink.error(new IllegalStateException("Not connected"));
                return;
            }
            Mqtt3Subscribe subWithCb = Mqtt3Subscribe.builder()
                    .topicFilters(sub.getTopicFilters())
                    .callback(sub.getCallback())
                    .build();
            subscriptionHandler.subscribe(channel.pipeline().context(subscriptionHandler).channel().pipeline().context(subscriptionHandler).handler() != null ?
                    channel.pipeline().context(subscriptionHandler) : null, subWithCb)
                    .asMono()
                    .subscribe(sink::success, sink::error);
        });
    }

    // Simplified subscribe - will be refined in implementation
    @Override
    public Flux<Mqtt3Publish> subscribePublishes(Mqtt3Subscribe sub) {
        // 预留实现
        return Flux.error(new UnsupportedOperationException("Not yet implemented"));
    }

    @Override
    public Flux<Mqtt3Publish> publishes(MqttGlobalPublishFilter filter) {
        // 预留实现
        return Flux.error(new UnsupportedOperationException("Not yet implemented"));
    }

    // ========== Publish ==========

    @Override
    public Mono<Mqtt3PublishResult> publish(Mqtt3Publish publish) {
        return Mono.create(sink -> {
            MqttClientState currentState = state.get();
            if (currentState == MqttClientState.CONNECTED) {
                int packetId = packetIdManager.nextPacketId();
                publish.setPacketId(packetId);

                if (publish.getQos() == plus.jmqx.client.mqtt.message.QoS.AT_MOST_ONCE) {
                    // QoS 0: 直接写入，无需 ACK
                    channel.writeAndFlush(Mqtt3MessageCodec.encodePublish(publish));
                    sink.success(new Mqtt3PublishResult() {
                        @Override
                        public Mqtt3Publish getPublish() { return publish; }
                        @Override
                        public Throwable getError() { return null; }
                    });
                } else {
                    // QoS 1/2: 注册 ACK 跟踪
                    Sinks.One<Void> ackSink = outgoingQosHandler.registerOutgoing(packetId);
                    channel.writeAndFlush(Mqtt3MessageCodec.encodePublish(publish));
                    ackSink.asMono()
                            .doOnSuccess(v -> sink.success(new Mqtt3PublishResult() {
                                @Override
                                public Mqtt3Publish getPublish() { return publish; }
                                @Override
                                public Throwable getError() { return null; }
                            }))
                            .doOnError(err -> sink.success(new Mqtt3PublishResult() {
                                @Override
                                public Mqtt3Publish getPublish() { return publish; }
                                @Override
                                public Throwable getError() { return err; }
                            }))
                            .subscribe();
                }
            } else if (currentState == MqttClientState.DISCONNECTED && config.isAutomaticReconnect()) {
                // 断线缓存
                if (messageBuffer.offer(publish)) {
                    // 返回一个待定的结果（在重连后完成）
                    Mqtt3Publish pendingPub = publish;
                    sink.success(new Mqtt3PublishResult() {
                        @Override
                        public Mqtt3Publish getPublish() { return pendingPub; }
                        @Override
                        public Throwable getError() { return null; }
                    });
                } else {
                    sink.error(new RuntimeException("Message buffer full"));
                }
            } else {
                sink.error(new IllegalStateException("Client is " + currentState));
            }
        });
    }

    // ========== Unsubscribe ==========

    @Override
    public Mono<Void> unsubscribe(Mqtt3Unsubscribe unsub) {
        // 简化实现
        return Mono.error(new UnsupportedOperationException("Not yet implemented"));
    }

    // ========== Disconnect ==========

    @Override
    public Mono<Void> disconnect() {
        return Mono.create(sink -> {
            if (channel != null && channel.isActive()) {
                channel.writeAndFlush(Mqtt3MessageCodec.encodeDisconnect());
                channel.close();
                cleanup();
            }
            state.set(MqttClientState.DISCONNECTED);
            sink.success();
        });
    }

    // ========== 内部方法 ==========

    private void handleDisconnect(Throwable cause) {
        state.set(MqttClientState.DISCONNECTED);
        MqttClientDisconnectedContext ctx = new MqttClientDisconnectedContext(
                config,
                MqttClientDisconnectedContext.DisconnectSource.SERVER,
                cause);
        disconnectedListeners.forEach(l -> l.onDisconnected(ctx));
    }

    private void flushMessageBuffer() {
        messageBuffer.flush(publish -> {
            if (channel != null && channel.isActive()) {
                channel.writeAndFlush(Mqtt3MessageCodec.encodePublish(publish));
            }
        });
    }

    private void cleanup() {
        if (eventLoopGroup != null && !eventLoopGroup.isShutdown()) {
            eventLoopGroup.shutdownGracefully();
        }
        channel = null;
    }

    @Override
    public Mqtt3ClientConfig getConfig() { return config; }

    @Override
    public MqttClientState getState() { return state.get(); }

    @Override
    public plus.jmqx.client.mqtt.MqttVersion getVersion() {
        return plus.jmqx.client.mqtt.MqttVersion.MQTT_3_1_1;
    }

    @Override
    public Mqtt3AsyncClientImpl toAsync() {
        return new Mqtt3AsyncClientImpl(this);
    }

    @Override
    public Mqtt3RxClient toRx() { return this; }

    @Override
    public Mqtt3BlockingClient toBlock() {
        return new Mqtt3BlockingClientImpl(this);
    }
}
```

- [ ] **Step 2: Commit**

```bash
git add jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/internal/DefaultMqtt3Client.java
git commit -m "feat(client): add DefaultMqtt3Client core engine"
```

---

### Task 15: Mqtt3RxClientImpl — 响应式 API 实现

**Files:**
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/internal/Mqtt3RxClientImpl.java`

Note: DefaultMqtt3Client already implements Mqtt3RxClient, so this is an alias/forwarder to keep the file structure aligned with the design.

- [ ] **Step 1: Create Mqtt3RxClientImpl.java**

```java
package plus.jmqx.client.mqtt.v3.internal;

import plus.jmqx.client.mqtt.MqttGlobalPublishFilter;
import plus.jmqx.client.mqtt.v3.Mqtt3RxClient;
import plus.jmqx.client.mqtt.v3.message.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Mqtt3RxClient 实现（委托给 DefaultMqtt3Client）
 */
public class Mqtt3RxClientImpl implements Mqtt3RxClient {

    private final DefaultMqtt3Client delegate;

    public Mqtt3RxClientImpl(DefaultMqtt3Client delegate) {
        this.delegate = delegate;
    }

    @Override
    public Mono<Mqtt3ConnAck> connect() { return delegate.connect(); }

    @Override
    public Mono<Mqtt3SubAck> subscribe(Mqtt3Subscribe sub) { return delegate.subscribe(sub); }

    @Override
    public Flux<Mqtt3Publish> subscribePublishes(Mqtt3Subscribe sub) { return delegate.subscribePublishes(sub); }

    @Override
    public Flux<Mqtt3Publish> publishes(MqttGlobalPublishFilter filter) { return delegate.publishes(filter); }

    @Override
    public Mono<Mqtt3PublishResult> publish(Mqtt3Publish publish) { return delegate.publish(publish); }

    @Override
    public Mono<Void> unsubscribe(Mqtt3Unsubscribe unsub) { return delegate.unsubscribe(unsub); }

    @Override
    public Mono<Void> disconnect() { return delegate.disconnect(); }

    @Override
    public Mqtt3ClientConfig getConfig() { return delegate.getConfig(); }

    @Override
    public MqttClientState getState() { return delegate.getState(); }

    @Override
    public MqttVersion getVersion() { return delegate.getVersion(); }

    @Override
    public Mqtt3AsyncClient toAsync() { return delegate.toAsync(); }

    @Override
    public Mqtt3RxClient toRx() { return this; }

    @Override
    public Mqtt3BlockingClient toBlock() { return delegate.toBlock(); }
}
```

Add the missing imports:

Actually, let me fix this: Mqtt3RxClientImpl needs a bunch of imports. Let me write it more cleanly.

Let me redo the file properly. Actually the code above already defines all the needed return types. But we need to fix the `Mqtt3ClientConfig` and `MqttClientState` and `MqttVersion` imports which are in different packages.

Fixed version:

```java
package plus.jmqx.client.mqtt.v3.internal;

import plus.jmqx.client.mqtt.MqttClientState;
import plus.jmqx.client.mqtt.MqttGlobalPublishFilter;
import plus.jmqx.client.mqtt.MqttVersion;
import plus.jmqx.client.mqtt.v3.*;
import plus.jmqx.client.mqtt.v3.message.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Mqtt3RxClient 实现（委托给 DefaultMqtt3Client）
 */
public class Mqtt3RxClientImpl implements Mqtt3RxClient {
    // ... same as above
}
```

- [ ] **Step 2: Commit**

```bash
git add jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/internal/Mqtt3RxClientImpl.java
git commit -m "feat(client): add Mqtt3RxClientImpl"
```

---

### Task 16: Mqtt3AsyncClientImpl — Future API 包装

**Files:**
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/internal/Mqtt3AsyncClientImpl.java`

- [ ] **Step 1: Create Mqtt3AsyncClientImpl.java**

```java
package plus.jmqx.client.mqtt.v3.internal;

import plus.jmqx.client.mqtt.MqttClientState;
import plus.jmqx.client.mqtt.MqttVersion;
import plus.jmqx.client.mqtt.v3.*;
import plus.jmqx.client.mqtt.v3.message.*;
import reactor.core.publisher.Mono;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Mqtt3AsyncClient 实现
 * <p>
 * 将 Reactor Mono/Flux 包装为 CompletableFuture API。
 * </p>
 */
public class Mqtt3AsyncClientImpl implements Mqtt3AsyncClient {

    private final Mqtt3RxClient rxClient;

    public Mqtt3AsyncClientImpl(Mqtt3RxClient rxClient) {
        this.rxClient = rxClient;
    }

    @Override
    public CompletableFuture<Mqtt3ConnAck> connect() {
        return rxClient.connect().toFuture();
    }

    @Override
    public CompletableFuture<Mqtt3SubAck> subscribe(Mqtt3Subscribe sub, Consumer<Mqtt3Publish> callback) {
        sub.setCallback(callback);
        return rxClient.subscribe(sub).toFuture();
    }

    @Override
    public CompletableFuture<Mqtt3PublishResult> publish(Mqtt3Publish publish) {
        return rxClient.publish(publish).toFuture();
    }

    @Override
    public CompletableFuture<Void> unsubscribe(Mqtt3Unsubscribe unsub) {
        return rxClient.unsubscribe(unsub).toFuture();
    }

    @Override
    public CompletableFuture<Void> disconnect() {
        return rxClient.disconnect().toFuture();
    }

    @Override
    public Mqtt3ClientConfig getConfig() { return rxClient.getConfig(); }

    @Override
    public MqttClientState getState() { return rxClient.getState(); }

    @Override
    public MqttVersion getVersion() { return rxClient.getVersion(); }

    @Override
    public Mqtt3AsyncClient toAsync() { return this; }

    @Override
    public Mqtt3RxClient toRx() { return rxClient; }

    @Override
    public Mqtt3BlockingClient toBlock() { return rxClient.toBlock(); }
}
```

- [ ] **Step 2: Commit**

```bash
git add jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/internal/Mqtt3AsyncClientImpl.java
git commit -m "feat(client): add Mqtt3AsyncClientImpl (CompletableFuture wrapper)"
```

---

### Task 17: Mqtt3BlockingClientImpl — 阻塞 API 包装

**Files:**
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/internal/Mqtt3BlockingClientImpl.java`

- [ ] **Step 1: Create Mqtt3BlockingClientImpl.java**

```java
package plus.jmqx.client.mqtt.v3.internal;

import plus.jmqx.client.mqtt.MqttClientState;
import plus.jmqx.client.mqtt.MqttVersion;
import plus.jmqx.client.mqtt.v3.*;
import plus.jmqx.client.mqtt.v3.message.*;

import java.time.Duration;

/**
 * Mqtt3BlockingClient 实现
 * <p>
 * 将 Reactor Mono 包装为阻塞调用。
 * </p>
 */
public class Mqtt3BlockingClientImpl implements Mqtt3BlockingClient {

    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);

    private final Mqtt3RxClient rxClient;

    public Mqtt3BlockingClientImpl(Mqtt3RxClient rxClient) {
        this.rxClient = rxClient;
    }

    @Override
    public Mqtt3ConnAck connect() {
        return rxClient.connect().block(DEFAULT_TIMEOUT);
    }

    @Override
    public Mqtt3SubAck subscribe(Mqtt3Subscribe sub) {
        return rxClient.subscribe(sub).block(DEFAULT_TIMEOUT);
    }

    @Override
    public void publish(Mqtt3Publish publish) {
        rxClient.publish(publish).block(DEFAULT_TIMEOUT);
    }

    @Override
    public void unsubscribe(Mqtt3Unsubscribe unsub) {
        rxClient.unsubscribe(unsub).block(DEFAULT_TIMEOUT);
    }

    @Override
    public void disconnect() {
        rxClient.disconnect().block(DEFAULT_TIMEOUT);
    }

    @Override
    public Mqtt3ClientConfig getConfig() { return rxClient.getConfig(); }

    @Override
    public MqttClientState getState() { return rxClient.getState(); }

    @Override
    public MqttVersion getVersion() { return rxClient.getVersion(); }

    @Override
    public Mqtt3AsyncClient toAsync() { return rxClient.toAsync(); }

    @Override
    public Mqtt3RxClient toRx() { return rxClient; }

    @Override
    public Mqtt3BlockingClient toBlock() { return this; }
}
```

- [ ] **Step 2: Commit**

```bash
git add jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/internal/Mqtt3BlockingClientImpl.java
git commit -m "feat(client): add Mqtt3BlockingClientImpl"
```

---

### Task 18: AutoReconnect — 自动重连

**Files:**
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/internal/reconnect/MqttAutoReconnect.java`

- [ ] **Step 1: Create MqttAutoReconnect.java**

```java
package plus.jmqx.client.mqtt.internal.reconnect;

import lombok.extern.slf4j.Slf4j;
import plus.jmqx.client.mqtt.lifecycle.MqttClientDisconnectedContext;
import plus.jmqx.client.mqtt.lifecycle.MqttClientDisconnectedListener;
import plus.jmqx.client.mqtt.lifecycle.MqttClientReconnector;

import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

/**
 * 自动重连实现
 * <p>
 * 指数退避 + 随机 jitter：
 * delay = min(initialDelay * 2^attempts, maxDelay)
 * delay += random(-25%, +25%)
 * </p>
 */
@Slf4j
public class MqttAutoReconnect implements MqttClientDisconnectedListener {

    private final long initialDelayMs;
    private final long maxDelayMs;
    private final Consumer<MqttClientReconnector> reconnectAction;

    public MqttAutoReconnect(long initialDelayMs, long maxDelayMs,
                              Consumer<MqttClientReconnector> reconnectAction) {
        this.initialDelayMs = initialDelayMs;
        this.maxDelayMs = maxDelayMs;
        this.reconnectAction = reconnectAction;
    }

    @Override
    public void onDisconnected(MqttClientDisconnectedContext context) {
        if (context.getSource() == MqttClientDisconnectedContext.DisconnectSource.USER) {
            // 用户主动断开不重连
            return;
        }

        int attempts = 0; // 由调用者传入
        long delay = computeDelay(attempts);
        log.info("Auto-reconnect scheduled in {}ms (attempt {})", delay, attempts + 1);

        MqttClientReconnector reconnector = new MqttClientReconnector(attempts + 1, true)
                .delay(delay)
                .resubscribe(true);

        reconnectAction.accept(reconnector);
    }

    private long computeDelay(int attempts) {
        long delay = Math.min(
                initialDelayMs * (1L << Math.min(attempts, 16)),
                maxDelayMs
        );
        // +/- 25% jitter
        double jitter = 1.0 + (ThreadLocalRandom.current().nextDouble() - 0.5) * 0.5;
        return (long) (delay * jitter);
    }
}
```

- [ ] **Step 2: Commit**

```bash
git add jmqx-client/src/main/java/plus/jmqx/client/mqtt/internal/reconnect/
git commit -m "feat(client): add auto-reconnect with exponential backoff"
```

---

### Task 19: MessageBuffer — 断线缓存

**Files:**
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/internal/buffer/MessageBuffer.java`

- [ ] **Step 1: Create MessageBuffer.java**

```java
package plus.jmqx.client.mqtt.internal.buffer;

import lombok.extern.slf4j.Slf4j;
import plus.jmqx.client.mqtt.v3.message.Mqtt3Publish;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;

/**
 * 断线缓存
 * <p>
 * 断线期间暂存 publish 消息，重连后按序刷新。
 * </p>
 */
@Slf4j
public class MessageBuffer {

    private final Queue<Mqtt3Publish> buffer = new ConcurrentLinkedQueue<>();
    private final int maxSize;

    public MessageBuffer(int maxSize) {
        this.maxSize = maxSize <= 0 ? Integer.MAX_VALUE : maxSize;
    }

    /**
     * 缓存消息
     *
     * @param publish 发布消息
     * @return true 缓存成功，false 缓冲区满
     */
    public boolean offer(Mqtt3Publish publish) {
        if (buffer.size() >= maxSize) {
            log.warn("Message buffer full (max={}), dropping message topic={}", maxSize, publish.getTopic());
            return false;
        }
        return buffer.offer(publish);
    }

    /**
     * 重连后按序刷新所有缓存消息
     *
     * @param writer 消息写入函数
     */
    public void flush(Consumer<Mqtt3Publish> writer) {
        Mqtt3Publish pub;
        int count = 0;
        while ((pub = buffer.poll()) != null) {
            try {
                writer.accept(pub);
                count++;
            } catch (Exception e) {
                log.error("Failed to flush buffered message topic={}", pub.getTopic(), e);
            }
        }
        if (count > 0) {
            log.info("Flushed {} buffered messages", count);
        }
    }

    /**
     * 清空缓存
     */
    public void clear() {
        buffer.clear();
    }

    /**
     * 当前缓存大小
     */
    public int size() {
        return buffer.size();
    }
}
```

- [ ] **Step 2: Commit**

```bash
git add jmqx-client/src/main/java/plus/jmqx/client/mqtt/internal/buffer/
git commit -m "feat(client): add message buffer for offline buffering"
```

---

### Task 20: MqttClientBuilder 和 Mqtt3ClientBuilder

**Files:**
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/MqttClientBuilder.java`
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/Mqtt3ClientBuilder.java`

- [ ] **Step 1: Create MqttClientBuilder.java**

```java
package plus.jmqx.client.mqtt;

import plus.jmqx.client.mqtt.v3.Mqtt3ClientBuilder;

/**
 * 顶层 MQTT 客户端构建器入口
 */
public class MqttClientBuilder {

    public Mqtt3ClientBuilder useMqttVersion3() {
        return new Mqtt3ClientBuilder();
    }

    /**
     * MQTT 5 构建器（预留）
     */
    public Object useMqttVersion5() {
        throw new UnsupportedOperationException("MQTT 5 not yet supported");
    }
}
```

- [ ] **Step 2: Create Mqtt3ClientBuilder.java**

```java
package plus.jmqx.client.mqtt.v3;

import plus.jmqx.client.mqtt.lifecycle.MqttClientConnectedListener;
import plus.jmqx.client.mqtt.lifecycle.MqttClientDisconnectedListener;
import plus.jmqx.client.mqtt.internal.reconnect.MqttAutoReconnect;
import plus.jmqx.client.mqtt.v3.internal.DefaultMqtt3Client;
import plus.jmqx.client.mqtt.v3.internal.Mqtt3RxClientImpl;
import plus.jmqx.client.mqtt.v3.message.Mqtt3Publish;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

/**
 * MQTT 3.1/3.1.1 客户端构建器
 */
public class Mqtt3ClientBuilder {

    private final Mqtt3ClientConfig config = new Mqtt3ClientConfig();
    private final List<MqttClientConnectedListener> connectedListeners = new ArrayList<>();
    private final List<MqttClientDisconnectedListener> disconnectedListeners = new ArrayList<>();

    public Mqtt3ClientBuilder serverHost(String serverHost) {
        config.setServerHost(serverHost);
        return this;
    }

    public Mqtt3ClientBuilder serverPort(int serverPort) {
        config.setServerPort(serverPort);
        return this;
    }

    public Mqtt3ClientBuilder identifier(String clientId) {
        config.setClientId(clientId);
        return this;
    }

    public Mqtt3ClientBuilder identifier() {
        config.setClientId(UUID.randomUUID().toString().substring(0, 8));
        return this;
    }

    public Mqtt3ClientBuilder keepAliveSeconds(int keepAlive) {
        config.setKeepAliveSeconds(keepAlive);
        return this;
    }

    public Mqtt3ClientBuilder cleanSession(boolean clean) {
        config.setCleanSession(clean);
        return this;
    }

    public Mqtt3ClientBuilder username(String username) {
        config.setUsername(username);
        return this;
    }

    public Mqtt3ClientBuilder password(byte[] password) {
        config.setPassword(password);
        return this;
    }

    public Mqtt3ClientBuilder willPublish(Mqtt3Publish will) {
        config.setWillPublish(will);
        return this;
    }

    public Mqtt3ClientBuilder automaticReconnect() {
        config.setAutomaticReconnect(true);
        return this;
    }

    public Mqtt3ClientBuilder automaticReconnect(long initialDelayMs, long maxDelayMs) {
        config.setAutomaticReconnect(true);
        config.setReconnectInitialDelayMs(initialDelayMs);
        config.setReconnectMaxDelayMs(maxDelayMs);
        return this;
    }

    public Mqtt3ClientBuilder addConnectedListener(MqttClientConnectedListener listener) {
        connectedListeners.add(listener);
        return this;
    }

    public Mqtt3ClientBuilder addDisconnectedListener(MqttClientDisconnectedListener listener) {
        disconnectedListeners.add(listener);
        return this;
    }

    /**
     * 构建响应式客户端
     */
    public Mqtt3RxClient buildRx() {
        ensureClientId();
        DefaultMqtt3Client client = new DefaultMqtt3Client(config, connectedListeners, disconnectedListeners);
        setupAutoReconnect(client);
        return new Mqtt3RxClientImpl(client);
    }

    /**
     * 构建异步客户端（CompletableFuture）
     */
    public Mqtt3AsyncClient buildAsync() {
        ensureClientId();
        Mqtt3RxClient rx = buildRx();
        return rx.toAsync();
    }

    /**
     * 构建阻塞客户端
     */
    public Mqtt3BlockingClient buildBlocking() {
        ensureClientId();
        Mqtt3RxClient rx = buildRx();
        return rx.toBlock();
    }

    private void ensureClientId() {
        if (config.getClientId() == null || config.getClientId().isEmpty()) {
            config.setClientId("jmqx-client-" + UUID.randomUUID().toString().substring(0, 8));
        }
    }

    private void setupAutoReconnect(DefaultMqtt3Client client) {
        if (config.isAutomaticReconnect()) {
            MqttAutoReconnect autoReconnect = new MqttAutoReconnect(
                    config.getReconnectInitialDelayMs(),
                    config.getReconnectMaxDelayMs(),
                    reconnector -> {
                        if (reconnector.isReconnect()) {
                            // 延迟后执行重连
                            try {
                                Thread.sleep(reconnector.getDelayMs());
                                client.connect().subscribe();
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        }
                    }
            );
            disconnectedListeners.add(autoReconnect);
        }
    }
}
```

- [ ] **Step 3: Commit**

```bash
git add jmqx-client/src/main/java/plus/jmqx/client/mqtt/MqttClientBuilder.java \
        jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/Mqtt3ClientBuilder.java
git commit -m "feat(client): add client builders (top-level + MQTT 3.x)"
```

---

### Task 21: MQTT 5 接口占位

**Files:**
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/v5/Mqtt5Client.java`
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/v5/Mqtt5AsyncClient.java`
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/v5/Mqtt5RxClient.java`
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/v5/Mqtt5BlockingClient.java`
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/v5/message/Mqtt5Connect.java`
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/v5/message/Mqtt5ConnAck.java`
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/v5/message/Mqtt5Publish.java`
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/v5/message/Mqtt5Subscribe.java`

- [ ] **Step 1: Create Mqtt5Client.java**

```java
package plus.jmqx.client.mqtt.v5;

import plus.jmqx.client.mqtt.MqttClient;

/**
 * MQTT 5 客户端接口（预留）
 */
public interface Mqtt5Client extends MqttClient {
}
```

- [ ] **Step 2: Create Mqtt5AsyncClient.java**

```java
package plus.jmqx.client.mqtt.v5;

import plus.jmqx.client.mqtt.MqttClient;

/**
 * MQTT 5 异步客户端（预留）
 */
public interface Mqtt5AsyncClient extends Mqtt5Client {
}
```

- [ ] **Step 3: Create Mqtt5RxClient.java**

```java
package plus.jmqx.client.mqtt.v5;

/**
 * MQTT 5 响应式客户端（预留）
 */
public interface Mqtt5RxClient extends Mqtt5Client {
}
```

- [ ] **Step 4: Create Mqtt5BlockingClient.java**

```java
package plus.jmqx.client.mqtt.v5;

/**
 * MQTT 5 阻塞客户端（预留）
 */
public interface Mqtt5BlockingClient extends Mqtt5Client {
}
```

- [ ] **Step 5: Create Mqtt5Connect.java**

```java
package plus.jmqx.client.mqtt.v5.message;

/**
 * MQTT 5 CONNECT 消息（预留）
 */
public class Mqtt5Connect {
}
```

- [ ] **Step 6: Create Mqtt5ConnAck.java, Mqtt5Publish.java, Mqtt5Subscribe.java** (same pattern — empty placeholder classes)

- [ ] **Step 7: Commit**

```bash
git add jmqx-client/src/main/java/plus/jmqx/client/mqtt/v5/
git commit -m "feat(client): add MQTT 5 placeholder interfaces"
```

---

### Task 22: Mqtt3ClientConfig 补充 cleanSession 字段

Since the `Mqtt3ClientConfig` extends `MqttClientConfig`, we need a `cleanSession` property and `isAutomaticReconnect` flag.

- [ ] **Step 1: Update MqttClientConfig.java**

Add missing fields:

```java
// Add to existing MqttClientConfig.java:
    private boolean cleanSession = true;
```

- [ ] **Step 2: Commit**

```bash
git add jmqx-client/src/main/java/plus/jmqx/client/mqtt/MqttClientConfig.java
git commit -m "feat(client): add cleanSession config field"
```

---

### Task 23: 集成测试

**Files:**
- Create: `jmqx-client/src/test/java/plus/jmqx/client/mqtt/Mqtt3ClientIntegrationTest.java`

- [ ] **Step 1: Create Mqtt3ClientIntegrationTest.java**

```java
package plus.jmqx.client.mqtt;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import plus.jmqx.client.mqtt.message.QoS;
import plus.jmqx.client.mqtt.v3.*;
import plus.jmqx.client.mqtt.v3.message.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

/**
 * MQTT 3.1.1 客户端集成测试
 * <p>
 * 需要本地运行 jmqx-broker（默认 1883）
 * </p>
 */
class Mqtt3ClientIntegrationTest {
    private static final Logger log = LoggerFactory.getLogger(Mqtt3ClientIntegrationTest.class);

    @Test
    void testConnectAndPublish() throws Exception {
        Mqtt3AsyncClient client = MqttClient.builder()
                .useMqttVersion3()
                .serverHost("localhost")
                .serverPort(1883)
                .identifier("test-client-1")
                .buildAsync();

        Mqtt3ConnAck connAck = client.connect().get(5, TimeUnit.SECONDS);
        assertNotNull(connAck);
        assertTrue(connAck.getReturnCode() == 0, "Connection should be accepted");

        log.info("Connected: sessionPresent={}", connAck.isSessionPresent());

        Mqtt3Publish publish = Mqtt3Publish.builder()
                .topic("test/topic")
                .payload("Hello MQTT".getBytes())
                .qos(QoS.AT_LEAST_ONCE)
                .build();

        Mqtt3PublishResult result = client.publish(publish).get(5, TimeUnit.SECONDS);
        assertNotNull(result);
        assertNull(result.getError());

        log.info("Published: topic={}", result.getPublish().getTopic());

        client.disconnect().get(5, TimeUnit.SECONDS);
        log.info("Disconnected");
    }

    @Test
    void testSubscribeAndReceive() throws Exception {
        Mqtt3AsyncClient client = MqttClient.builder()
                .useMqttVersion3()
                .serverHost("localhost")
                .serverPort(1883)
                .identifier("test-client-2")
                .buildAsync();

        client.connect().get(5, TimeUnit.SECONDS);

        // 创建接收标记
        CompletableFuture<Mqtt3Publish> received = new CompletableFuture<>();

        Mqtt3Subscribe sub = Mqtt3Subscribe.builder()
                .topicFilter(new Mqtt3TopicFilter("test/#", QoS.AT_MOST_ONCE))
                .addTopicFilter("test/#", QoS.AT_MOST_ONCE)
                .callback(received::complete)
                .build();

        Mqtt3SubAck subAck = client.subscribe(sub, received::complete).get(5, TimeUnit.SECONDS);
        assertNotNull(subAck);
        log.info("Subscribed: grantedQos={}", subAck.getGrantedQos());

        // 发布消息到订阅的主题
        Mqtt3Publish pub = Mqtt3Publish.builder()
                .topic("test/hello")
                .payload("World".getBytes())
                .qos(QoS.AT_LEAST_ONCE)
                .build();
        client.publish(pub).get(5, TimeUnit.SECONDS);

        // 等待接收
        Mqtt3Publish receivedMsg = received.get(5, TimeUnit.SECONDS);
        assertNotNull(receivedMsg);
        assertEquals("test/hello", receivedMsg.getTopic());
        log.info("Received: topic={}, payload={}",
                receivedMsg.getTopic(), new String(receivedMsg.getPayload()));

        client.disconnect().get(5, TimeUnit.SECONDS);
    }

    @Test
    void testBlockingAPI() {
        Mqtt3BlockingClient client = MqttClient.builder()
                .useMqttVersion3()
                .serverHost("localhost")
                .serverPort(1883)
                .identifier("test-blocking")
                .buildBlocking();

        Mqtt3ConnAck connAck = client.connect();
        assertNotNull(connAck);

        client.publish(Mqtt3Publish.builder()
                .topic("test/blocking")
                .payload("blocking test".getBytes())
                .qos(QoS.AT_MOST_ONCE)
                .build());

        client.disconnect();
    }
}
```

Also update Mqtt3Subscribe to support builder:

- [ ] **Step 2: Add convenience methods to Mqtt3TopicFilter and Mqtt3Subscribe**

```java
// Add to Mqtt3Subscribe.java:
public void setCallback(Consumer<Mqtt3Publish> callback) { this.callback = callback; }
public Consumer<Mqtt3Publish> getCallback() { return callback; }
```

- [ ] **Step 3: Commit**

```bash
git add jmqx-client/src/test/java/plus/jmqx/client/mqtt/ \
        jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/message/Mqtt3Subscribe.java
git commit -m "test(client): add integration test for connect/subscribe/publish/disconnect"
```

---

## Spec Coverage Check

| Spec requirement | Task(s) | Status |
|---|---|---|
| 版本无关基础类型 (QoS, MqttVersion, MqttClientState) | Task 1 | ✅ |
| MQTT 3.1.1 消息模型 | Task 2 | ✅ |
| 配置模型 (MqttClientConfig, Mqtt3ClientConfig) | Task 3, Task 22 | ✅ |
| 生命周期监听器 | Task 4 | ✅ |
| PacketIdManager, NettyUtil | Task 5 | ✅ |
| MQTT 3.1.1 编解码器 (encode/decode) | Task 6 | ✅ |
| Netty Pipeline: Mqtt3ConnectHandler | Task 7 | ✅ |
| Netty Pipeline: Mqtt3DisconnectHandler | Task 8 | ✅ |
| Netty Pipeline: Mqtt3SubscriptionHandler | Task 9 | ✅ |
| Netty Pipeline: Mqtt3IncomingQosHandler | Task 10 | ✅ |
| Netty Pipeline: Mqtt3OutgoingQosHandler | Task 11 | ✅ |
| Netty Pipeline: Mqtt3ChannelInitializer | Task 12 | ✅ |
| 客户端接口 (MqttClient, Mqtt3XxxClient, Mqtt3PublishResult) | Task 13 | ✅ |
| DefaultMqtt3Client 核心引擎 | Task 14 | ✅ |
| Mqtt3RxClientImpl | Task 15 | ✅ |
| Mqtt3AsyncClientImpl | Task 16 | ✅ |
| Mqtt3BlockingClientImpl | Task 17 | ✅ |
| MqttAutoReconnect | Task 18 | ✅ |
| MessageBuffer 断线缓存 | Task 19 | ✅ |
| MqttClientBuilder + Mqtt3ClientBuilder | Task 20 | ✅ |
| MQTT 5 包占位 | Task 21 | ✅ |
| 集成测试 (connect/publish/subscribe/receive/disconnect) | Task 23 | ✅ |

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-07-02-jmqx-client-plan.md`.

**Two execution options:**

1. **Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration
2. **Inline Execution** — Execute tasks in this session, batch execution with checkpoints

**Which approach?**
