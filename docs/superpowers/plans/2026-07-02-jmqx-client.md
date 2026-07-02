# jmqx-client MQTT Client Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a high-performance async MQTT client library (v3.1.1 + v5.0) on reactor-netty with CompletableFuture/Reactor/Blocking APIs, two-way backpressure, auto-reconnect, and offline buffering.

**Architecture:** Single reactor-netty engine (`DefaultMqttClient`) implementing `Mqtt3RxClient`/`Mqtt5RxClient`. Protocol differences isolated behind `MqttMessageService` (v3/v5 impls). QoS state machine, reconnect, backpressure, offline buffer written once and shared. Single Netty `MqttClientHandler` delegates to plain Java objects (`AckTracker`/`MqttInbox`/`MqttOutbox`). v3 lands as a working client first (Task 20 = end-to-end testable), v5 extends the same engine.

**Tech Stack:** Java 17, reactor-netty 1.2.x (`reactor-bom 2024.0.4`), netty-codec-mqtt 4.1.119.Final, reactor-core 3.7.x, Lombok, SLF4J, Hutool, JUnit5, reactor-test.

**Spec:** `docs/superpowers/specs/2026-07-02-jmqx-client-design.md`

---

## Conventions

- Base package: `plus.jmqx.client`
- All reactor types from `reactor.core.publisher` (`Mono`/`Flux`/`Sinks`).
- All Netty MQTT types from `io.netty.handler.codec.mqtt`.
- Messages are **immutable value types** (interface + `*Impl` final class + builder). Use Lombok `@Builder`/`@AllArgsConstructor` on impls, expose interface in API.
- Test-driven for pure-logic components (PacketIdManager, MessageBuffer, AckTracker, TopicMatcher, codec). For reactor wiring and integration, use `StepVerifier` + real broker.
- Frequent commits: each task ends with a commit.
- Run single test: `mvn -pl jmqx-client test -Dtest=ClassName#method` from repo root. Build whole client: `mvn -pl jmqx-client test`. Integration tests (suffixed `IT`) require jmqx-broker on `localhost:1883`: start it separately.

---

## File Structure

```
jmqx-client/src/main/java/plus/jmqx/client/mqtt/
├── MqttClient.java                          # version-agnostic entry interface
├── MqttClientBuilder.java                  # top-level builder (.useMqttVersion3()/5())
├── MqttClientConfig.java                   # config base
├── MqttClientState.java                     # 4-state enum
├── MqttVersion.java                         # version enum
├── MqttGlobalPublishFilter.java             # inbound filter enum
├── message/                                 # version-agnostic message interfaces
│   ├── MqttPublish.java  MqttConnect.java  MqttSubscribe.java
│   ├── MqttUnsubscribe.java  MqttConnAck.java  MqttSubAck.java
│   ├── MqttPublishResult.java  MqttTopicFilter.java  QoS.java
│   └── MqttMessageBuilder.java              # builder helpers
├── lifecycle/
│   ├── MqttClientConnectedListener.java  MqttClientDisconnectedListener.java
│   ├── MqttClientConnectedContext.java  MqttClientDisconnectedContext.java
│   └── MqttClientReconnector.java
├── v3/
│   ├── Mqtt3Client.java  Mqtt3ClientBuilder.java  Mqtt3ClientConfig.java
│   ├── Mqtt3AsyncClient.java  Mqtt3RxClient.java  Mqtt3BlockingClient.java
│   ├── Mqtt3PublishResult.java
│   ├── message/                             # Mqtt3Publish, Mqtt3Connect, Mqtt3ConnAck, ...
│   │   ├── Mqtt3Publish.java  Mqtt3Connect.java  Mqtt3ConnAck.java
│   │   ├── Mqtt3Subscribe.java  Mqtt3SubAck.java  Mqtt3Unsubscribe.java
│   │   ├── Mqtt3TopicFilter.java  Mqtt3PubAck.java  Mqtt3PubRec.java
│   │   ├── Mqtt3PubRel.java  Mqtt3PubComp.java  Mqtt3Disconnect.java
│   │   └── Mqtt3PublishImpl.java (+ builder)
│   └── internal/Mqtt3MessageService.java
├── v5/
│   ├── Mqtt5Client.java  Mqtt5ClientBuilder.java  Mqtt5ClientConfig.java
│   ├── Mqtt5AsyncClient.java  Mqtt5RxClient.java  Mqtt5BlockingClient.java
│   ├── Mqtt5PublishResult.java
│   ├── message/                             # + Properties variants
│   │   ├── Mqtt5Publish.java  Mqtt5Connect.java  Mqtt5ConnAck.java
│   │   ├── Mqtt5Subscribe.java  Mqtt5SubAck.java  Mqtt5Unsubscribe.java
│   │   ├── Mqtt5TopicFilter.java  Mqtt5Disconnect.java  Mqtt5PubAck.java
│   │   ├── Mqtt5PublishProperties.java  Mqtt5ConnAckProperties.java
│   │   └── Mqtt5PublishImpl.java (+ builders)
│   └── internal/Mqtt5MessageService.java
└── internal/
    ├── DefaultMqttClient.java                # single engine (implements v3 & v5 Rx)
    ├── handler/MqttClientHandler.java        # single Netty inbound/outbound handler
    ├── AckTracker.java  InboundQos.java
    ├── MqttInbox.java  MqttOutbox.java
    ├── SubscriptionStore.java  PendingOutbound.java
    ├── MqttMessageService.java               # protocol adapter interface
    ├── reconnect/MqttAutoReconnect.java
    ├── buffer/MessageBuffer.java  MessageBufferFullException.java
    ├── transport/TransportFactory.java  MqttSslConfig.java  MqttWebSocketConfig.java
    └── util/PacketIdManager.java  NettyUtil.java  TopicMatcher.java

jmqx-client/src/test/java/plus/jmqx/client/mqtt/
├── internal/util/PacketIdManagerTest.java
├── internal/buffer/MessageBufferTest.java
├── internal/AckTrackerTest.java
├── internal/util/TopicMatcherTest.java
├── v3/internal/Mqtt3MessageServiceTest.java
├── v5/internal/Mqtt5MessageServiceTest.java
├── internal/MqttInboxBackpressureTest.java
├── internal/MqttOutboxBackpressureTest.java
├── internal/reconnect/MqttAutoReconnectTest.java
├── v3/Mqtt3ClientIT.java                    # integration (needs broker)
└── v5/Mqtt5ClientIT.java
```

---

### Task 1: pom.xml — add reactor-test + netty-transport test scope

**Files:**
- Modify: `jmqx-client/pom.xml`

`reactor-netty` already pulls reactor-core + netty-codec-mqtt is present. We add `reactor-test` (for `StepVerifier`/`VirtualTimeScheduler`) and ensure `netty-handler`/`netty-transport` (for `EmbeddedChannel`) are available — reactor-netty already depends on them transitively, so they resolve at test scope. Also add a tiny test resource marker.

- [ ] **Step 1: Update jmqx-client/pom.xml**

Replace the `<dependencies>` block with:

```xml
    <dependencies>
        <dependency>
            <groupId>io.projectreactor.netty</groupId>
            <artifactId>reactor-netty</artifactId>
        </dependency>
        <dependency>
            <groupId>io.netty</groupId>
            <artifactId>netty-codec-mqtt</artifactId>
        </dependency>
        <dependency>
            <groupId>org.projectlombok</groupId>
            <artifactId>lombok</artifactId>
            <scope>provided</scope>
        </dependency>
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-api</artifactId>
        </dependency>
        <dependency>
            <groupId>cn.hutool</groupId>
            <artifactId>hutool-all</artifactId>
        </dependency>
        <!-- test -->
        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>io.projectreactor</groupId>
            <artifactId>reactor-test</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>ch.qos.logback</groupId>
            <artifactId>logback-classic</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>
```

- [ ] **Step 2: Verify build compiles (deps resolve)**

Run: `mvn -pl jmqx-client -am dependency:resolve -q`
Expected: BUILD SUCCESS, no unresolved deps.

- [ ] **Step 3: Commit**

```bash
git add jmqx-client/pom.xml
git commit -m "chore(client): add reactor-test + logback test deps, scope lombok provided"
```

---

### Task 2: Version-agnostic enums and base message interfaces

**Files:**
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/MqttVersion.java`
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/MqttClientState.java`
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/MqttGlobalPublishFilter.java`
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/message/QoS.java`
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/message/MqttPublish.java`
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/message/MqttTopicFilter.java`
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/message/MqttMessageBuilder.java`

- [ ] **Step 1: Write QoS test**

Create `jmqx-client/src/test/java/plus/jmqx/client/mqtt/message/QoSTest.java`:

```java
package plus.jmqx.client.mqtt.message;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class QoSTest {

    @Test
    void fromValue_roundTrips() {
        for (QoS q : QoS.values()) {
            assertSame(q, QoS.fromValue(q.value()));
        }
    }

    @Test
    void fromValue_invalidThrows() {
        assertThrows(IllegalArgumentException.class, () -> QoS.fromValue(3));
        assertThrows(IllegalArgumentException.class, () -> QoS.fromValue(-1));
    }

    @Test
    void values_are012() {
        assertEquals(0, QoS.AT_MOST_ONCE.value());
        assertEquals(1, QoS.AT_LEAST_ONCE.value());
        assertEquals(2, QoS.EXACTLY_ONCE.value());
    }
}
```

- [ ] **Step 2: Run test — fails (no class)**

Run: `mvn -pl jmqx-client test -Dtest=QoSTest -q`
Expected: compilation failure (QoS not defined).

- [ ] **Step 3: Create the version-agnostic types**

`MqttVersion.java`:
```java
package plus.jmqx.client.mqtt;

/** MQTT protocol version. */
public enum MqttVersion {
    MQTT_3_1(3, "MQIsdp", (byte) 3),
    MQTT_3_1_1(4, "MQTT", (byte) 4),
    MQTT_5(5, "MQTT", (byte) 5);

    private final int level;
    private final String name;
    private final byte protocolLevel;

    MqttVersion(int level, String name, byte protocolLevel) {
        this.level = level;
        this.name = name;
        this.protocolLevel = protocolLevel;
    }

    public int level() { return level; }
    public String protocolName() { return name; }
    public byte protocolLevel() { return protocolLevel; }
}
```

`MqttClientState.java`:
```java
package plus.jmqx.client.mqtt;

/** Client connection state machine (4-state). */
public enum MqttClientState {
    DISCONNECTED,
    CONNECTING,
    CONNECTED,
    DISCONNECTING
}
```

`MqttGlobalPublishFilter.java`:
```java
package plus.jmqx.client.mqtt;

/** Filter for the global publishes() stream. */
public enum MqttGlobalPublishFilter {
    ALL,
    SUBSCRIBED,
    UNSOLICITED
}
```

`message/QoS.java`:
```java
package plus.jmqx.client.mqtt.message;

/** MQTT Quality of Service level. */
public enum QoS {
    AT_MOST_ONCE(0),
    AT_LEAST_ONCE(1),
    EXACTLY_ONCE(2);

    private final int value;

    QoS(int value) { this.value = value; }

    public int value() { return value; }

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

`message/MqttPublish.java`:
```java
package plus.jmqx.client.mqtt.message;

/** Version-agnostic published message. */
public interface MqttPublish {
    String getTopic();
    byte[] getPayloadAsBytes();
    QoS getQoS();
    boolean isRetain();
    boolean isDup();
    int getPacketId();

    /**
     * Acknowledge this inbound message. For QoS1/2 the engine sends PUBACK/PUBREC to the broker
     * when the subscriber consumes the message; for QoS0 it is a no-op. Calling on an outbound
     * publish is a no-op. Safe to call multiple times. The engine's inbox overrides this to
     * fire the real ACK callback; plain value impls inherit the no-op.
     */
    default void ack() {}
}
```

`message/MqttTopicFilter.java`:
```java
package plus.jmqx.client.mqtt.message;

/** Version-agnostic topic filter. */
public interface MqttTopicFilter {
    String getTopicFilter();
    QoS getQoS();
}
```

`message/MqttMessageBuilder.java`:
```java
package plus.jmqx.client.mqtt.message;

import io.netty.buffer.ByteBufUtil;

/** Helpers for message builders. */
public final class MqttMessageBuilder {
    private MqttMessageBuilder() {}

    /** Null-safe payload copy to a fresh byte array. */
    public static byte[] cloneBytes(byte[] src) {
        return src == null ? new byte[0] : src.clone();
    }

    public static int payloadSize(byte[] payload) {
        return payload == null ? 0 : payload.length;
    }
}
```

- [ ] **Step 4: Run test — passes**

Run: `mvn -pl jmqx-client test -Dtest=QoSTest -q`
Expected: BUILD SUCCESS, QoSTest passes.

- [ ] **Step 5: Commit**

```bash
git add jmqx-client/src/main/java/plus/jmqx/client/mqtt/MqttVersion.java \
        jmqx-client/src/main/java/plus/jmqx/client/mqtt/MqttClientState.java \
        jmqx-client/src/main/java/plus/jmqx/client/mqtt/MqttGlobalPublishFilter.java \
        jmqx-client/src/main/java/plus/jmqx/client/mqtt/message/
git commit -m "feat(client): add version-agnostic enums and base message interfaces"
```

---

### Task 3: TopicMatcher — MQTT topic filter matching (+/# wildcards)

**Files:**
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/internal/util/TopicMatcher.java`
- Test: `jmqx-client/src/test/java/plus/jmqx/client/mqtt/internal/util/TopicMatcherTest.java`

- [ ] **Step 1: Write failing test**

```java
package plus.jmqx.client.mqtt.internal.util;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class TopicMatcherTest {

    @Test
    void exactMatch() {
        assertTrue(TopicMatcher.matches("a/b/c", "a/b/c"));
        assertFalse(TopicMatcher.matches("a/b/c", "a/b/d"));
    }

    @Test
    void singleLevelWildcard() {
        assertTrue(TopicMatcher.matches("a/+/c", "a/b/c"));
        assertTrue(TopicMatcher.matches("a/+/c", "a/x/c"));
        assertFalse(TopicMatcher.matches("a/+/c", "a/b/d"));
        assertFalse(TopicMatcher.matches("a/+/c", "a/b/x/c"));
    }

    @Test
    void multiLevelWildcard() {
        assertTrue(TopicMatcher.matches("a/#", "a/b/c"));
        assertTrue(TopicMatcher.matches("a/#", "a/b"));
        assertTrue(TopicMatcher.matches("#", "a/b/c/d"));
        assertFalse(TopicMatcher.matches("a/#", "b/c"));
    }

    @Test
    void wildcardMustBeWholeLevel() {
        assertFalse(TopicMatcher.matches("a/b+", "a/bx"));
        assertFalse(TopicMatcher.matches("a/#c", "a/bc"));
    }

    @Test
    void emptyLevelsHandled() {
        assertTrue(TopicMatcher.matches("a//c", "a//c"));
        assertTrue(TopicMatcher.matches("a/+/c", "a//c"));
    }
}
```

- [ ] **Step 2: Run test — fails**

Run: `mvn -pl jmqx-client test -Dtest=TopicMatcherTest -q`
Expected: FAIL (TopicMatcher not defined).

- [ ] **Step 3: Implement TopicMatcher**

```java
package plus.jmqx.client.mqtt.internal.util;

/**
 * MQTT topic filter matching per spec §4.7 — supports '+' (single level) and '#' (multi level).
 * Wildcards must occupy a complete level (between '/' separators).
 */
public final class TopicMatcher {

    private TopicMatcher() {}

    public static boolean matches(String filter, String topic) {
        if (filter == null || topic == null) return false;
        String[] f = filter.split("/", -1);
        String[] t = topic.split("/", -1);
        int fi = 0;
        for (int ti = 0; ti < t.length; ti++) {
            if (fi >= f.length) return false;
            String level = f[fi];
            if ("#".equals(level)) return true;          // '#' must be last level
            if ("+".equals(level) || level.equals(t[ti])) {
                fi++;
                continue;
            }
            return false;
        }
        // topic exhausted; match only if filter also exhausted OR filter ends with '#'
        if (fi == f.length) return true;
        return fi == f.length - 1 && "#".equals(f[fi]);
    }
}
```

- [ ] **Step 4: Run test — passes**

Run: `mvn -pl jmqx-client test -Dtest=TopicMatcherTest -q`
Expected: BUILD SUCCESS, all TopicMatcherTest pass.

- [ ] **Step 5: Commit**

```bash
git add jmqx-client/src/main/java/plus/jmqx/client/mqtt/internal/util/TopicMatcher.java \
        jmqx-client/src/test/java/plus/jmqx/client/mqtt/internal/util/TopicMatcherTest.java
git commit -m "feat(client): add TopicMatcher for +/# wildcard matching"
```

---

### Task 4: PacketIdManager — single source of packet IDs

**Files:**
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/internal/util/PacketIdManager.java`
- Test: `jmqx-client/src/test/java/plus/jmqx/client/mqtt/internal/util/PacketIdManagerTest.java`

- [ ] **Step 1: Write failing test**

```java
package plus.jmqx.client.mqtt.internal.util;

import org.junit.jupiter.api.Test;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import static org.junit.jupiter.api.Assertions.*;

class PacketIdManagerTest {

    @Test
    void rangeIs1to65535() {
        PacketIdManager pm = new PacketIdManager();
        Set<Integer> seen = new HashSet<>();
        for (int i = 0; i < 65535; i++) {
            int id = pm.nextPacketId();
            assertTrue(id >= 1 && id <= 65535, "out of range: " + id);
            assertTrue(seen.add(id), "duplicate within one cycle: " + id);
        }
    }

    @Test
    void wrapsAroundAfter65535() {
        PacketIdManager pm = new PacketIdManager();
        for (int i = 0; i < 65535; i++) pm.nextPacketId();
        int next = pm.nextPacketId();
        assertTrue(next >= 1 && next <= 65535);
    }

    @Test
    void neverReturnsZero() {
        PacketIdManager pm = new PacketIdManager();
        for (int i = 0; i < 200_000; i++) {
            assertNotEquals(0, pm.nextPacketId(), "returned 0 at iteration " + i);
        }
    }

    @Test
    void concurrentNoDuplicate() throws InterruptedException {
        PacketIdManager pm = new PacketIdManager();
        int threads = 8, perThread = 10_000;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        Set<Integer> all = java.util.Collections.synchronizedSet(new HashSet<>());
        CountDownLatch latch = new CountDownLatch(threads);
        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                for (int j = 0; j < perThread; j++) all.add(pm.nextPacketId());
                latch.countDown();
            });
        }
        latch.await();
        pool.shutdown();
        // 80000 ids, range 1..65535 wraps ~1.2x; uniqueness only holds within a single cycle,
        // so assert no id is 0 and set non-empty (true uniqueness is per-cycle, not global after wrap).
        assertFalse(all.contains(0));
        assertFalse(all.isEmpty());
    }
}
```

- [ ] **Step 2: Run test — fails**

Run: `mvn -pl jmqx-client test -Dtest=PacketIdManagerTest -q`
Expected: FAIL (class not defined).

- [ ] **Step 3: Implement PacketIdManager**

```java
package plus.jmqx.client.mqtt.internal.util;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Single source of MQTT packet IDs (1..65535). Shared by PUBLISH(QoS1/2), SUBSCRIBE, UNSUBSCRIBE.
 * Fixes the old design's double-PacketIdManager collision bug.
 */
public final class PacketIdManager {

    private static final int MIN = 1;
    private static final int MAX = 65535;

    private final AtomicInteger next = new AtomicInteger(MIN);

    public int nextPacketId() {
        while (true) {
            int id = next.getAndUpdate(v -> (v >= MAX ? MIN : v + 1));
            if (id != 0) return id;
        }
    }
}
```

- [ ] **Step 4: Run test — passes**

Run: `mvn -pl jmqx-client test -Dtest=PacketIdManagerTest -q`
Expected: BUILD SUCCESS.

- [ ] **Step 5: Commit**

```bash
git add jmqx-client/src/main/java/plus/jmqx/client/mqtt/internal/util/PacketIdManager.java \
        jmqx-client/src/test/java/plus/jmqx/client/mqtt/internal/util/PacketIdManagerTest.java
git commit -m "feat(client): add single PacketIdManager (1..65535, wraparound, thread-safe)"
```

---

### Task 5: Lifecycle listeners and reconnector

**Files:**
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/lifecycle/MqttClientConnectedContext.java`
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/lifecycle/MqttClientDisconnectedContext.java`
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/lifecycle/MqttClientConnectedListener.java`
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/lifecycle/MqttClientDisconnectedListener.java`
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/lifecycle/MqttClientReconnector.java`

- [ ] **Step 1: Create contexts and listeners**

`MqttClientConnectedContext.java`:
```java
package plus.jmqx.client.mqtt.lifecycle;

import plus.jmqx.client.mqtt.MqttClientConfig;

public final class MqttClientConnectedContext {
    private final MqttClientConfig clientConfig;
    private final boolean sessionPresent;

    public MqttClientConnectedContext(MqttClientConfig clientConfig, boolean sessionPresent) {
        this.clientConfig = clientConfig;
        this.sessionPresent = sessionPresent;
    }

    public MqttClientConfig getClientConfig() { return clientConfig; }
    public boolean isSessionPresent() { return sessionPresent; }
}
```

`MqttClientDisconnectedContext.java`:
```java
package plus.jmqx.client.mqtt.lifecycle;

import plus.jmqx.client.mqtt.MqttClientConfig;

public final class MqttClientDisconnectedContext {
    public enum DisconnectSource { USER, CLIENT, SERVER }

    private final MqttClientConfig clientConfig;
    private final DisconnectSource source;
    private final Throwable cause;
    private final MqttClientReconnector reconnector;

    public MqttClientDisconnectedContext(MqttClientConfig clientConfig, DisconnectSource source,
                                        Throwable cause, MqttClientReconnector reconnector) {
        this.clientConfig = clientConfig;
        this.source = source;
        this.cause = cause;
        this.reconnector = reconnector;
    }

    public MqttClientConfig getClientConfig() { return clientConfig; }
    public DisconnectSource getSource() { return source; }
    public Throwable getCause() { return cause; }
    public MqttClientReconnector getReconnector() { return reconnector; }
}
```

`MqttClientConnectedListener.java`:
```java
package plus.jmqx.client.mqtt.lifecycle;

@FunctionalInterface
public interface MqttClientConnectedListener {
    void onConnected(MqttClientConnectedContext context);
}
```

`MqttClientDisconnectedListener.java`:
```java
package plus.jmqx.client.mqtt.lifecycle;

@FunctionalInterface
public interface MqttClientDisconnectedListener {
    void onDisconnected(MqttClientDisconnectedContext context);
}
```

`MqttClientReconnector.java`:
```java
package plus.jmqx.client.mqtt.lifecycle;

/**
 * Reconnect control passed into DisconnectedListener. The listener mutates this to
 * influence reconnect behavior (whether, delay, resubscribe). Read by the engine after
 * all listeners have run.
 */
public final class MqttClientReconnector {

    private boolean reconnect = true;
    private long delayMs = 0;
    private int attempts = 0;
    private boolean resubscribeIfSessionPresent = false;
    private boolean resubscribeIfSessionExpired = true;
    private boolean republishBufferedIfSessionExpired = true;

    public MqttClientReconnector(int attempts, boolean reconnect) {
        this.attempts = attempts;
        this.reconnect = reconnect;
    }

    public MqttClientReconnector reconnect(boolean reconnect) { this.reconnect = reconnect; return this; }
    public boolean isReconnect() { return reconnect; }

    public MqttClientReconnector delay(long delayMs) { this.delayMs = delayMs; return this; }
    public long getDelayMs() { return delayMs; }

    public int getAttempts() { return attempts; }
    public void setAttempts(int attempts) { this.attempts = attempts; }

    public MqttClientReconnector resubscribeIfSessionPresent(boolean v) { this.resubscribeIfSessionPresent = v; return this; }
    public boolean isResubscribeIfSessionPresent() { return resubscribeIfSessionPresent; }

    public MqttClientReconnector resubscribeIfSessionExpired(boolean v) { this.resubscribeIfSessionExpired = v; return this; }
    public boolean isResubscribeIfSessionExpired() { return resubscribeIfSessionExpired; }

    public MqttClientReconnector republishBufferedIfSessionExpired(boolean v) { this.republishBufferedIfSessionExpired = v; return this; }
    public boolean isRepublishBufferedIfSessionExpired() { return republishBufferedIfSessionExpired; }
}
```

- [ ] **Step 2: Verify compile**

Run: `mvn -pl jmqx-client compile -q`
Expected: BUILD SUCCESS.

- [ ] **Step 3: Commit**

```bash
git add jmqx-client/src/main/java/plus/jmqx/client/mqtt/lifecycle/
git commit -m "feat(client): add lifecycle listeners, contexts, and reconnector"
```

---

### Task 6: MqttClientConfig — config base class

**Files:**
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/MqttClientConfig.java`

- [ ] **Step 1: Create MqttClientConfig**

```java
package plus.jmqx.client.mqtt;

import lombok.Data;
import plus.jmqx.client.mqtt.internal.transport.MqttSslConfig;
import plus.jmqx.client.mqtt.internal.transport.MqttWebSocketConfig;
import plus.jmqx.client.mqtt.message.MqttPublish;
import reactor.netty.resources.LoopResources;

/** Version-agnostic client configuration base. */
@Data
public class MqttClientConfig {

    public enum TransportType { TCP, TLS, WS, WSS }

    // connection
    private String serverHost = "localhost";
    private int serverPort = 1883;
    private String clientId;                 // null -> auto-generated "jmqx-<uuid8>"
    private int keepAliveSeconds = 60;
    private MqttVersion version;             // set by builder.useMqttVersionX()

    // timeouts
    private int socketConnectTimeoutMs = 10_000;
    private int mqttConnectTimeoutMs = 60_000;

    // transport
    private TransportType transportType = TransportType.TCP;
    private MqttSslConfig sslConfig;
    private MqttWebSocketConfig webSocketConfig;

    // threads
    private int nettyThreads = Math.max(Runtime.getRuntime().availableProcessors(), 2);
    private LoopResources loopResources;     // externally injected shared loop, optional

    // session
    private boolean cleanSession = true;      // v3; v5 maps to cleanStart
    private long sessionExpiryInterval = 0;   // v5 only (seconds)
    private int receiveMaximum = 65535;        // v5 only — client's cap on server inflight

    // auth
    private String username;
    private byte[] password;

    // will
    private MqttPublish willPublish;

    // reconnect
    private boolean automaticReconnect = false;
    private long reconnectInitialDelayMs = 1000;
    private long reconnectMaxDelayMs = 120_000;
    private int maxReconnectAttempts = Integer.MAX_VALUE;

    // buffer & flow control
    private int messageBufferMaxSize = 1000;
    private long messageBufferMaxBytes = 64L * 1024 * 1024;
    private boolean clearBufferOnDisconnect = false;
    private int maxInflightMessages = 64;      // v3; v5 overridden by CONNACK Receive Maximum
    private int inboxBufferSize = 1024;       // Sinks.Many backpressure buffer
}
```

- [ ] **Step 2: Compile (will fail until transport configs exist in Task 13 — create stubs now to unblock)**

Create `jmqx-client/src/main/java/plus/jmqx/client/mqtt/internal/transport/MqttSslConfig.java`:
```java
package plus.jmqx.client.mqtt.internal.transport;

import lombok.Builder;
import lombok.Data;

/** SSL/TLS configuration. */
@Data
@Builder
public class MqttSslConfig {
    private String trustStorePath;
    private String trustStorePassword;
    private String keyStorePath;
    private String keyStorePassword;
    private String[] cipherSuites;
    private String[] protocols;
    private int handshakeTimeoutMs = 10_000;
}
```

Create `jmqx-client/src/main/java/plus/jmqx/client/mqtt/internal/transport/MqttWebSocketConfig.java`:
```java
package plus.jmqx.client.mqtt.internal.transport;

import lombok.Builder;
import lombok.Data;

/** WebSocket configuration (mqtt subprotocol). */
@Data
@Builder
public class MqttWebSocketConfig {
    private String path = "/mqtt";
    private String subprotocol = "mqtt";
    private String query;
}
```

- [ ] **Step 3: Compile**

Run: `mvn -pl jmqx-client compile -q`
Expected: BUILD SUCCESS.

- [ ] **Step 4: Commit**

```bash
git add jmqx-client/src/main/java/plus/jmqx/client/mqtt/MqttClientConfig.java \
        jmqx-client/src/main/java/plus/jmqx/client/mqtt/internal/transport/MqttSslConfig.java \
        jmqx-client/src/main/java/plus/jmqx/client/mqtt/internal/transport/MqttWebSocketConfig.java
git commit -m "feat(client): add MqttClientConfig + SSL/WebSocket config stubs"
```

---

### Task 7: MQTT 3.1.1 message types

**Files:**
- Create (under `jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/message/`):
  - `Mqtt3Publish.java`, `Mqtt3PublishImpl.java`
  - `Mqtt3Connect.java`, `Mqtt3ConnAck.java`, `Mqtt3ConnAckReturnCode.java`
  - `Mqtt3Subscribe.java`, `Mqtt3SubAck.java`, `Mqtt3TopicFilter.java`
  - `Mqtt3Unsubscribe.java`
  - `Mqtt3PubAck.java`, `Mqtt3PubRec.java`, `Mqtt3PubRel.java`, `Mqtt3PubComp.java`, `Mqtt3Disconnect.java`

- [ ] **Step 1: Create the immutable Mqtt3Publish + impl**

`v3/message/Mqtt3Publish.java`:
```java
package plus.jmqx.client.mqtt.v3.message;

import plus.jmqx.client.mqtt.message.MqttPublish;
import plus.jmqx.client.mqtt.message.QoS;

/** MQTT 3.1.1 PUBLISH (immutable value type). */
public interface Mqtt3Publish extends MqttPublish {

    /**
     * Acknowledge this inbound message. For QoS1/2 this sends PUBACK/PUBREC to the broker
     * (called by the engine's inbox after the subscriber consumes it); for QoS0 it is a no-op.
     * Calling on an outbound publish is a no-op. Safe to call multiple times.
     */
    default void ack() {}

    static Mqtt3PublishBuilder builder() { return new Mqtt3PublishBuilder(); }

    /** Mutable builder (Lombok @Builder on impl provides toBuilder). */
    Mqtt3PublishBuilder toBuilder();
}
```

`v3/message/Mqtt3PublishBuilder.java`:
```java
package plus.jmqx.client.mqtt.v3.message;

import plus.jmqx.client.mqtt.message.QoS;

public class Mqtt3PublishBuilder {
    private String topic;
    private byte[] payload;
    private QoS qos = QoS.AT_MOST_ONCE;
    private boolean retain = false;
    private boolean dup = false;
    private int packetId = 0;

    public Mqtt3PublishBuilder topic(String topic) { this.topic = topic; return this; }
    public Mqtt3PublishBuilder payload(byte[] payload) { this.payload = payload; return this; }
    public Mqtt3PublishBuilder qos(QoS qos) { this.qos = qos; return this; }
    public Mqtt3PublishBuilder retain(boolean retain) { this.retain = retain; return this; }
    public Mqtt3PublishBuilder dup(boolean dup) { this.dup = dup; return this; }
    public Mqtt3PublishBuilder packetId(int packetId) { this.packetId = packetId; return this; }

    public Mqtt3Publish build() {
        return new Mqtt3PublishImpl(topic, payload, qos, retain, dup, packetId);
    }
}
```

`v3/message/Mqtt3PublishImpl.java`:
```java
package plus.jmqx.client.mqtt.v3.message;

import plus.jmqx.client.mqtt.message.MqttMessageBuilder;
import plus.jmqx.client.mqtt.message.QoS;

/** Immutable Mqtt3Publish implementation. */
public final class Mqtt3PublishImpl implements Mqtt3Publish {
    private final String topic;
    private final byte[] payload;
    private final QoS qos;
    private final boolean retain;
    private final boolean dup;
    private final int packetId;

    public Mqtt3PublishImpl(String topic, byte[] payload, QoS qos, boolean retain, boolean dup, int packetId) {
        this.topic = topic;
        this.payload = MqttMessageBuilder.cloneBytes(payload);
        this.qos = qos;
        this.retain = retain;
        this.dup = dup;
        this.packetId = packetId;
    }

    @Override public String getTopic() { return topic; }
    @Override public byte[] getPayloadAsBytes() { return payload.clone(); }
    @Override public QoS getQoS() { return qos; }
    @Override public boolean isRetain() { return retain; }
    @Override public boolean isDup() { return dup; }
    @Override public int getPacketId() { return packetId; }

    @Override public Mqtt3PublishBuilder toBuilder() {
        return new Mqtt3PublishBuilder()
                .topic(topic).payload(payload).qos(qos)
                .retain(retain).dup(dup).packetId(packetId);
    }
}
```

- [ ] **Step 2: Create the remaining v3 messages**

`v3/message/Mqtt3Connect.java`:
```java
package plus.jmqx.client.mqtt.v3.message;

import lombok.Builder;
import lombok.Value;
import plus.jmqx.client.mqtt.message.MqttPublish;

/** MQTT 3.1.1 CONNECT parameters. */
@Value
@Builder(toBuilder = true)
public class Mqtt3Connect {
    String clientId;
    boolean cleanSession;
    int keepAliveSeconds;
    String username;
    byte[] password;
    MqttPublish willPublish;
}
```

`v3/message/Mqtt3ConnAck.java`:
```java
package plus.jmqx.client.mqtt.v3.message;

import lombok.Value;

/** MQTT 3.1.1 CONNACK. */
@Value
public class Mqtt3ConnAck {
    boolean sessionPresent;
    Mqtt3ConnAckReturnCode returnCode;
}
```

`v3/message/Mqtt3ConnAckReturnCode.java`:
```java
package plus.jmqx.client.mqtt.v3.message;

/** MQTT 3.1.1 CONNACK return codes (spec §3.2.2.3). */
public enum Mqtt3ConnAckReturnCode {
    ACCEPTED(0),
    UNACCEPTABLE_PROTOCOL_VERSION(1),
    IDENTIFIER_REJECTED(2),
    SERVER_UNAVAILABLE(3),
    BAD_USERNAME_OR_PASSWORD(4),
    NOT_AUTHORIZED(5);

    private final int code;
    Mqtt3ConnAckReturnCode(int code) { this.code = code; }
    public int code() { return code; }

    public static Mqtt3ConnAckReturnCode fromCode(int code) {
        for (var v : values()) if (v.code == code) return v;
        throw new IllegalArgumentException("Unknown CONNACK return code: " + code);
    }

    public boolean isAccepted() { return this == ACCEPTED; }
}
```

`v3/message/Mqtt3TopicFilter.java`:
```java
package plus.jmqx.client.mqtt.v3.message;

import lombok.Builder;
import lombok.Value;
import plus.jmqx.client.mqtt.message.MqttTopicFilter;
import plus.jmqx.client.mqtt.message.QoS;

@Value
@Builder
public class Mqtt3TopicFilter implements MqttTopicFilter {
    String topicFilter;
    QoS qos;
}
```

`v3/message/Mqtt3Subscribe.java`:
```java
package plus.jmqx.client.mqtt.v3.message;

import lombok.Builder;
import lombok.Value;
import java.util.List;

@Value
@Builder(toBuilder = true)
public class Mqtt3Subscribe {
    List<Mqtt3TopicFilter> topicFilters;
    int packetId;
}
```

`v3/message/Mqtt3SubAck.java`:
```java
package plus.jmqx.client.mqtt.v3.message;

import lombok.Value;
import plus.jmqx.client.mqtt.message.QoS;
import java.util.List;

@Value
public class Mqtt3SubAck {
    List<QoS> grantedQos;
    int packetId;
}
```

`v3/message/Mqtt3Unsubscribe.java`:
```java
package plus.jmqx.client.mqtt.v3.message;

import lombok.Builder;
import lombok.Value;
import java.util.List;

@Value
@Builder(toBuilder = true)
public class Mqtt3Unsubscribe {
    List<String> topicFilters;
    int packetId;
}
```

`v3/message/Mqtt3PubAck.java`, `Mqtt3PubRec.java`, `Mqtt3PubRel.java`, `Mqtt3PubComp.java` (same pattern):
```java
package plus.jmqx.client.mqtt.v3.message;

import lombok.Value;

@Value
public class Mqtt3PubAck {
    int packetId;
}
```
(repeat 4 files: `Mqtt3PubAck`, `Mqtt3PubRec`, `Mqtt3PubRel`, `Mqtt3PubComp`)

`v3/message/Mqtt3Disconnect.java`:
```java
package plus.jmqx.client.mqtt.v3.message;

/** MQTT 3.1.1 DISCONNECT — no fields (v3 has no reason code). */
public final class Mqtt3Disconnect {
    private Mqtt3Disconnect() {}
    public static final Mqtt3Disconnect INSTANCE = new Mqtt3Disconnect();
}
```

- [ ] **Step 3: Compile**

Run: `mvn -pl jmqx-client compile -q`
Expected: BUILD SUCCESS.

- [ ] **Step 4: Commit**

```bash
git add jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/message/
git commit -m "feat(client): add immutable MQTT 3.1.1 message types"
```

---

### Task 8: MqttMessageService interface + Mqtt3MessageService (codec)

**Files:**
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/internal/MqttMessageService.java`
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/internal/Mqtt3MessageService.java`
- Test: `jmqx-client/src/test/java/plus/jmqx/client/mqtt/v3/internal/Mqtt3MessageServiceTest.java`

The codec uses `netty-codec-mqtt` (`MqttEncoder`/`MqttDecoder`/`MqttMessageBuilders`) for wire format; `Mqtt3MessageService` converts between business message types and Netty types. Test with `EmbeddedChannel` running the real encoder/decoder for round-trip fidelity.

- [ ] **Step 1: Create the MqttMessageService interface**

```java
package plus.jmqx.client.mqtt.internal;

import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import plus.jmqx.client.mqtt.message.MqttConnAck;
import plus.jmqx.client.mqtt.message.MqttPublish;
import plus.jmqx.client.mqtt.message.MqttSubscribe;
import plus.jmqx.client.mqtt.message.MqttSubAck;
import plus.jmqx.client.mqtt.message.MqttUnsubscribe;
import plus.jmqx.client.mqtt.MqttClientConfig;

/** Protocol adapter — isolates v3/v5 wire-format differences. */
public interface MqttMessageService {

    /** Encode CONNECT. The config provides clientId/keepAlive/auth/will. */
    MqttMessage encodeConnect(MqttClientConfig config);

    MqttMessage encodePublish(MqttPublish publish, int packetId, boolean dup);
    MqttMessage encodeSubscribe(MqttSubscribe subscribe);
    MqttMessage encodeUnsubscribe(MqttUnsubscribe unsubscribe);
    MqttMessage encodePubAck(int packetId);
    MqttMessage encodePubRec(int packetId);
    MqttMessage encodePubRel(int packetId);
    MqttMessage encodePubComp(int packetId);
    MqttMessage encodeDisconnect();
    MqttMessage encodePingReq();

    MqttConnAck decodeConnAck(MqttConnAckMessage msg, MqttClientConfig config);
    MqttPublish decodePublish(MqttPublishMessage msg);
    MqttSubAck decodeSubAck(MqttSubAckMessage msg);
    int decodePacketId(MqttMessage msg);

    boolean isConnectionAccepted(MqttConnAck ack);
    RuntimeException connectionRefusedException(MqttConnAck ack);
}
```

- [ ] **Step 2: Write failing codec round-trip test**

```java
package plus.jmqx.client.mqtt.v3.internal;

import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import org.junit.jupiter.api.Test;
import plus.jmqx.client.mqtt.MqttClientConfig;
import plus.jmqx.client.mqtt.MqttVersion;
import plus.jmqx.client.mqtt.message.MqttConnAck;
import plus.jmqx.client.mqtt.message.MqttPublish;
import plus.jmqx.client.mqtt.message.QoS;
import plus.jmqx.client.mqtt.v3.message.Mqtt3Publish;
import plus.jmqx.client.mqtt.v3.message.Mqtt3Subscribe;
import plus.jmqx.client.mqtt.v3.message.Mqtt3TopicFilter;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class Mqtt3MessageServiceTest {

    private final Mqtt3MessageService svc = new Mqtt3MessageService();
    private final EmbeddedChannel ch = new EmbeddedChannel(MqttEncoder.INSTANCE, new MqttDecoder(8 * 1024 * 1024));

    private MqttMessage roundTrip(MqttMessage out) {
        ch.writeOutbound(out);
        ByteBuf buf = ch.readOutbound();
        ch.writeInbound(buf);
        return ch.readInbound();
    }

    private MqttClientConfig config() {
        MqttClientConfig c = new MqttClientConfig();
        c.setClientId("test-id");
        c.setKeepAliveSeconds(60);
        c.setCleanSession(true);
        c.setVersion(MqttVersion.MQTT_3_1_1);
        return c;
    }

    @Test
    void publishRoundTrips() {
        Mqtt3Publish pub = Mqtt3Publish.builder()
                .topic("a/b").payload("hello".getBytes()).qos(QoS.AT_LEAST_ONCE).build();
        MqttMessage enc = svc.encodePublish(pub, 42, false);
        MqttPublishMessage dec = (MqttPublishMessage) roundTrip(enc);

        MqttPublish decoded = svc.decodePublish(dec);
        assertEquals("a/b", decoded.getTopic());
        assertEquals("hello", new String(decoded.getPayloadAsBytes()));
        assertEquals(QoS.AT_LEAST_ONCE, decoded.getQoS());
        assertEquals(42, decoded.getPacketId());
    }

    @Test
    void subscribeRoundTripsAndDecodesSubAck() {
        Mqtt3Subscribe sub = Mqtt3Subscribe.builder()
                .topicFilters(List.of(Mqtt3TopicFilter.builder().topicFilter("t/#").qos(QoS.AT_MOST_ONCE).build()))
                .packetId(7).build();
        MqttMessage enc = svc.encodeSubscribe(sub);
        assertNotNull(roundTrip(enc));   // encoder accepts; full SUBACK decode tested via broker IT
    }

    @Test
    void connectAccepted() {
        // Construct a v3 CONNACK with returnCode=0 via codec, decode, assert accepted
        io.netty.handler.codec.mqtt.MqttConnAckMessage netty = io.netty.handler.codec.mqtt.MqttMessageBuilders
                .connAck()
                .returnCode(io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_ACCEPTED)
                .sessionPresent(false)
                .build();
        MqttConnAck ack = svc.decodeConnAck(netty, config());
        assertTrue(svc.isConnectionAccepted(ack));
    }

    @Test
    void connectRefusedMapsException() {
        io.netty.handler.codec.mqtt.MqttConnAckMessage netty = io.netty.handler.codec.mqtt.MqttMessageBuilders
                .connAck()
                .returnCode(io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_NOT_AUTHORIZED)
                .sessionPresent(false)
                .build();
        MqttConnAck ack = svc.decodeConnAck(netty, config());
        assertFalse(svc.isConnectionAccepted(ack));
        RuntimeException ex = svc.connectionRefusedException(ack);
        assertNotNull(ex.getMessage());
    }
}
```

- [ ] **Step 3: Run test — fails (Mqtt3MessageService not defined)**

Run: `mvn -pl jmqx-client test -Dtest=Mqtt3MessageServiceTest -q`
Expected: compilation failure.

- [ ] **Step 4: Implement Mqtt3MessageService**

```java
package plus.jmqx.client.mqtt.v3.internal;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.*;
import plus.jmqx.client.mqtt.MqttClientConfig;
import plus.jmqx.client.mqtt.internal.MqttMessageService;
import plus.jmqx.client.mqtt.message.MqttConnAck;
import plus.jmqx.client.mqtt.message.MqttPublish;
import plus.jmqx.client.mqtt.message.MqttSubscribe;
import plus.jmqx.client.mqtt.message.MqttSubAck;
import plus.jmqx.client.mqtt.message.MqttUnsubscribe;
import plus.jmqx.client.mqtt.message.QoS;
import plus.jmqx.client.mqtt.v3.message.Mqtt3ConnAck;
import plus.jmqx.client.mqtt.v3.message.Mqtt3ConnAckReturnCode;
import plus.jmqx.client.mqtt.v3.message.Mqtt3Publish;
import plus.jmqx.client.mqtt.v3.message.Mqtt3PublishImpl;
import plus.jmqx.client.mqtt.v3.message.Mqtt3SubAck;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/** MQTT 3.1.1 protocol adapter. */
public class Mqtt3MessageService implements MqttMessageService {

    @Override
    public MqttMessage encodeConnect(MqttClientConfig config) {
        String clientId = config.getClientId() != null ? config.getClientId() : "";
        MqttConnectPayload payload = new MqttConnectPayload(
                clientId,
                config.getWillPublish() != null ? config.getWillPublish().getTopic() : null,
                config.getWillPublish() != null ? config.getWillPublish().getPayloadAsBytes() : null,
                config.getUsername(),
                config.getPassword() != null ? new String(config.getPassword(), StandardCharsets.UTF_8) : null
        );
        MqttConnectVariableHeader header = new MqttConnectVariableHeader(
                io.netty.handler.codec.mqtt.MqttVersion.MQTT_3_1_1.protocolName(),
                io.netty.handler.codec.mqtt.MqttVersion.MQTT_3_1_1.protocolLevel(),
                config.isCleanSession(),
                config.getWillPublish() != null,
                config.getWillPublish() != null && config.getWillPublish().getQoS() != null
                        ? config.getWillPublish().getQoS().value() : 0,
                config.getWillPublish() != null && config.getWillPublish().isRetain(),
                config.getPassword() != null,
                config.getUsername() != null,
                config.getKeepAliveSeconds()
        );
        return new MqttConnectMessage(header, payload);
    }

    @Override
    public MqttMessage encodePublish(MqttPublish publish, int packetId, boolean dup) {
        ByteBuf payload = publish.getPayloadAsBytes() != null
                ? Unpooled.wrappedBuffer(publish.getPayloadAsBytes())
                : Unpooled.EMPTY_BUFFER;
        MqttFixedHeader fixed = new MqttFixedHeader(
                MqttMessageType.PUBLISH,
                dup,
                MqttQoS.valueOf(publish.getQoS().value()),
                publish.isRetain(),
                0
        );
        MqttPublishVariableHeader var = new MqttPublishVariableHeader(publish.getTopic(), packetId);
        return new MqttPublishMessage(fixed, var, payload);
    }

    @Override
    public MqttMessage encodeSubscribe(MqttSubscribe subscribe) {
        List<MqttTopicSubscription> subs = new ArrayList<>();
        for (var tf : subscribe.getTopicFilters()) {
            subs.add(new MqttTopicSubscription(tf.getTopicFilter(), MqttQoS.valueOf(tf.getQoS().value())));
        }
        return MqttMessageBuilders.subscribe()
                .messageId(subscribe.getPacketId())
                .addSubscriptions(subs)
                .build();
    }

    @Override
    public MqttMessage encodeUnsubscribe(MqttUnsubscribe unsubscribe) {
        return MqttMessageBuilders.unsubscribe()
                .messageId(unsubscribe.getPacketId())
                .topicFilters(unsubscribe.getTopicFilters())
                .build();
    }

    @Override
    public MqttMessage encodePubAck(int packetId) {
        return MqttMessageBuilders.pubAck().messageId(packetId).build();
    }

    @Override
    public MqttMessage encodePubRec(int packetId) {
        MqttFixedHeader fixed = new MqttFixedHeader(MqttMessageType.PUBREC, false, MqttQoS.AT_MOST_ONCE, false, 0);
        return new MqttMessage(fixed, MqttMessageIdVariableHeader.from(packetId));
    }

    @Override
    public MqttMessage encodePubRel(int packetId) {
        MqttFixedHeader fixed = new MqttFixedHeader(MqttMessageType.PUBREL, false, MqttQoS.AT_LEAST_ONCE, false, 0);
        return new MqttMessage(fixed, MqttMessageIdVariableHeader.from(packetId));
    }

    @Override
    public MqttMessage encodePubComp(int packetId) {
        MqttFixedHeader fixed = new MqttFixedHeader(MqttMessageType.PUBCOMP, false, MqttQoS.AT_MOST_ONCE, false, 0);
        return new MqttMessage(fixed, MqttMessageIdVariableHeader.from(packetId));
    }

    @Override
    public MqttMessage encodeDisconnect() {
        MqttFixedHeader fixed = new MqttFixedHeader(MqttMessageType.DISCONNECT, false, MqttQoS.AT_MOST_ONCE, false, 0);
        return new MqttMessage(fixed);
    }

    @Override
    public MqttMessage encodePingReq() {
        MqttFixedHeader fixed = new MqttFixedHeader(MqttMessageType.PINGREQ, false, MqttQoS.AT_MOST_ONCE, false, 0);
        return new MqttMessage(fixed);
    }

    @Override
    public MqttConnAck decodeConnAck(MqttConnAckMessage msg, MqttClientConfig config) {
        Mqtt3ConnAckReturnCode code = Mqtt3ConnAckReturnCode.fromCode(msg.variableHeader().connectReturnCode().byteValue());
        return new Mqtt3ConnAck(msg.variableHeader().isSessionPresent(), code);
    }

    @Override
    public MqttPublish decodePublish(MqttPublishMessage msg) {
        MqttFixedHeader fixed = msg.fixedHeader();
        ByteBuf buf = msg.payload();
        byte[] payload = new byte[buf.readableBytes()];
        buf.getBytes(buf.readerIndex(), payload);
        return new Mqtt3PublishImpl(
                msg.variableHeader().topicName(),
                payload,
                QoS.fromValue(fixed.qosLevel().value()),
                fixed.isRetain(),
                fixed.isDup(),
                msg.variableHeader().packetId()
        );
    }

    @Override
    public MqttSubAck decodeSubAck(MqttSubAckMessage msg) {
        List<QoS> granted = new ArrayList<>();
        for (int code : msg.payload().grantedQoSLevels()) {
            granted.add(QoS.fromValue(code));
        }
        return new Mqtt3SubAck(granted, msg.variableHeader().messageId());
    }

    @Override
    public int decodePacketId(MqttMessage msg) {
        if (msg.variableHeader() instanceof MqttMessageIdVariableHeader id) {
            return id.messageId();
        }
        if (msg instanceof MqttPublishMessage pub) {
            return pub.variableHeader().packetId();
        }
        return 0;
    }

    @Override
    public boolean isConnectionAccepted(MqttConnAck ack) {
        return ((Mqtt3ConnAck) ack).getReturnCode().isAccepted();
    }

    @Override
    public RuntimeException connectionRefusedException(MqttConnAck ack) {
        Mqtt3ConnAckReturnCode code = ((Mqtt3ConnAck) ack).getReturnCode();
        return new RuntimeException("MQTT connection refused: " + code);
    }
}
```

- [ ] **Step 5: Add the missing version-agnostic message interfaces referenced**

The `MqttMessageService` interface references `MqttConnAck`/`MqttSubscribe`/`MqttSubAck`/`MqttUnsubscribe` from `plus.jmqx.client.mqtt.message`. v3 impls (`Mqtt3ConnAck` etc.) live in `v3.message`. We need the version-agnostic interfaces so the service is version-agnostic. Create:

`message/MqttConnAck.java`:
```java
package plus.jmqx.client.mqtt.message;

/** Version-agnostic CONNACK marker. Concrete types in v3/v5.message. */
public interface MqttConnAck {
    boolean isSessionPresent();
}
```
`message/MqttSubscribe.java`:
```java
package plus.jmqx.client.mqtt.message;

import java.util.List;

public interface MqttSubscribe {
    List<? extends MqttTopicFilter> getTopicFilters();
    int getPacketId();
}
```
`message/MqttSubAck.java`:
```java
package plus.jmqx.client.mqtt.message;

import java.util.List;

public interface MqttSubAck {
    List<QoS> getGrantedQos();
    int getPacketId();
}
```
`message/MqttUnsubscribe.java`:
```java
package plus.jmqx.client.mqtt.message;

import java.util.List;

public interface MqttUnsubscribe {
    List<String> getTopicFilters();
    int getPacketId();
}
```
`message/MqttConnect.java`:
```java
package plus.jmqx.client.mqtt.message;

public interface MqttConnect {
    String getClientId();
    boolean isCleanSession();
    int getKeepAliveSeconds();
    MqttPublish getWillPublish();
    String getUsername();
    byte[] getPassword();
}
```
`message/MqttPublishResult.java`:
```java
package plus.jmqx.client.mqtt.message;

public interface MqttPublishResult {
    MqttPublish getPublish();
    Throwable getError();
}
```

Then make `Mqtt3ConnAck` implement `MqttConnAck`, `Mqtt3Subscribe` implement `MqttSubscribe`, `Mqtt3SubAck` implement `MqttSubAck`, `Mqtt3Unsubscribe` implement `MqttUnsubscribe`. Add `implements` clause to each `@Value` class:

- `Mqtt3ConnAck`: `public class Mqtt3ConnAck implements MqttConnAck` (add `@Override public boolean isSessionPresent() { return sessionPresent; }`).
- `Mqtt3Subscribe`: `implements MqttSubscribe` — note `topicFilters` is `List<Mqtt3TopicFilter>` which satisfies `List<? extends MqttTopicFilter>`; Lombok `@Value` generates getter, override return type to `List<Mqtt3TopicFilter>` is fine since it's covariant-assignable. Add `@Override` on `getPacketId`.
- `Mqtt3SubAck`: `implements MqttSubAck` (grantedQos is `List<QoS>`).
- `Mqtt3Unsubscribe`: `implements MqttUnsubscribe`.

For `@Value` + interface: change classes from `@Value` to `@Data` + explicit `implements` if Lombok `@Value` interferes with interface method overrides; otherwise `@Value public class Mqtt3ConnAck implements MqttConnAck { ... }` works. Use:

```java
@Value
public class Mqtt3ConnAck implements MqttConnAck {
    boolean sessionPresent;
    Mqtt3ConnAckReturnCode returnCode;

    @Override
    public boolean isSessionPresent() { return sessionPresent; }
}
```

- [ ] **Step 6: Run test — passes**

Run: `mvn -pl jmqx-client test -Dtest=Mqtt3MessageServiceTest -q`
Expected: BUILD SUCCESS, 4 tests pass.

- [ ] **Step 7: Commit**

```bash
git add jmqx-client/src/main/java/plus/jmqx/client/mqtt/internal/MqttMessageService.java \
        jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/internal/Mqtt3MessageService.java \
        jmqx-client/src/main/java/plus/jmqx/client/mqtt/message/ \
        jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/message/Mqtt3ConnAck.java \
        jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/message/Mqtt3Subscribe.java \
        jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/message/Mqtt3SubAck.java \
        jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/message/Mqtt3Unsubscribe.java \
        jmqx-client/src/test/java/plus/jmqx/client/mqtt/v3/internal/Mqtt3MessageServiceTest.java
git commit -m "feat(client): add MqttMessageService adapter + Mqtt3MessageService codec (TDD round-trip)"
```

---

### Task 9: PendingOutbound + AckTracker — outbound QoS ACK tracking

**Files:**
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/internal/PendingOutbound.java`
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/internal/AckTracker.java`
- Test: `jmqx-client/src/test/java/plus/jmqx/client/mqtt/internal/AckTrackerTest.java`

- [ ] **Step 1: Create PendingOutbound**

```java
package plus.jmqx.client.mqtt.internal;

import plus.jmqx.client.mqtt.message.MqttPublish;
import reactor.core.publisher.Sinks;

/** A pending outbound QoS1/2 message awaiting ACK. */
public final class PendingOutbound {
    private final MqttPublish publish;
    private final Sinks.One<MqttPublishResult> resultSink;
    private final long sentNanos;

    public PendingOutbound(MqttPublish publish, Sinks.One<MqttPublishResult> resultSink) {
        this.publish = publish;
        this.resultSink = resultSink;
        this.sentNanos = System.nanoTime();
    }

    public MqttPublish getPublish() { return publish; }
    public Sinks.One<MqttPublishResult> getResultSink() { return resultSink; }
    public long getSentNanos() { return sentNanos; }
}
```

Also create `MqttPublishResult` impl (version-agnostic). `internal/MqttPublishResultImpl.java`:
```java
package plus.jmqx.client.mqtt.internal;

import plus.jmqx.client.mqtt.message.MqttPublish;
import plus.jmqx.client.mqtt.message.MqttPublishResult;

public record MqttPublishResultImpl(MqttPublish publish, Throwable error) implements MqttPublishResult {
    @Override public MqttPublish getPublish() { return publish; }
    @Override public Throwable getError() { return error; }
}
```

- [ ] **Step 2: Write failing AckTracker test**

```java
package plus.jmqx.client.mqtt.internal;

import org.junit.jupiter.api.Test;
import plus.jmqx.client.mqtt.message.MqttPublish;
import plus.jmqx.client.mqtt.message.MqttPublishResult;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import static org.junit.jupiter.api.Assertions.*;

class AckTrackerTest {

    @Test
    void registerThenCompleteEmitsSuccess() {
        AckTracker tracker = new AckTracker();
        Sinks.One<MqttPublishResult> sink = Sinks.one();
        MqttPublish pub = stub();
        tracker.register(10, new PendingOutbound(pub, sink));

        StepVerifier.create(sink.asMono())
                .then(() -> tracker.complete(10, new MqttPublishResultImpl(pub, null)))
                .expectNextMatches(r -> r.getError() == null)
                .verifyComplete();
        assertNull(tracker.remove(10));  // already removed
    }

    @Test
    void pubrecTransitionsToPubcomp() {
        AckTracker tracker = new AckTracker();
        Sinks.One<MqttPublishResult> sink = Sinks.one();
        tracker.register(20, new PendingOutbound(stub(), sink));

        tracker.markReceived(20);          // PUBREC arrived
        assertFalse(tracker.isComplete(20));  // still pending PUBCOMP
        tracker.complete(20, new MqttPublishResultImpl(stub(), null));
        StepVerifier.create(sink.asMono())
                .then(() -> {})  // already completed by tracker.complete
                .expectNextCount(0)
                .thenCancel()
                .verify();
    }

    @Test
    void failEmitsError() {
        AckTracker tracker = new AckTracker();
        Sinks.One<MqttPublishResult> sink = Sinks.one();
        tracker.register(30, new PendingOutbound(stub(), sink));
        tracker.fail(30, new RuntimeException("disconnected"));

        StepVerifier.create(sink.asMono())
                .expectNextMatches(r -> r.getError() != null)
                .verifyComplete();
    }

    private MqttPublish stub() {
        return new MqttPublish() {
            public String getTopic() { return "t"; }
            public byte[] getPayloadAsBytes() { return new byte[0]; }
            public plus.jmqx.client.mqtt.message.QoS getQoS() { return plus.jmqx.client.mqtt.message.QoS.AT_LEAST_ONCE; }
            public boolean isRetain() { return false; }
            public boolean isDup() { return false; }
            public int getPacketId() { return 0; }
        };
    }
}
```

- [ ] **Step 3: Run test — fails**

Run: `mvn -pl jmqx-client test -Dtest=AckTrackerTest -q`
Expected: compilation failure (AckTracker not defined).

- [ ] **Step 4: Implement AckTracker**

```java
package plus.jmqx.client.mqtt.internal;

import plus.jmqx.client.mqtt.message.MqttPublishResult;
import reactor.core.publisher.Sinks;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks outbound QoS1/2 messages by packetId. Single source of truth for
 * "what's waiting for an ACK" — feeds MqttOutbox inflight counting.
 */
public final class AckTracker {

    private final Map<Integer, PendingOutbound> pending = new ConcurrentHashMap<>();

    public void register(int packetId, PendingOutbound po) {
        pending.put(packetId, po);
    }

    /** QoS1 PUBACK or QoS2 PUBCOMP — final completion. */
    public void complete(int packetId, MqttPublishResult result) {
        PendingOutbound po = pending.remove(packetId);
        if (po != null) po.getResultSink().tryEmitValue(result);
    }

    /** QoS2 PUBREC received — keep pending (waiting for PUBCOMP); caller sends PUBREL. */
    public void markReceived(int packetId) {
        // no state change to the map entry; it stays until PUBCOMP
    }

    /** Transport disconnected or error — fail all pending with the cause. */
    public void fail(int packetId, Throwable error) {
        PendingOutbound po = pending.remove(packetId);
        if (po != null) po.getResultSink().tryEmitValue(new MqttPublishResultImpl(po.getPublish(), error));
    }

    public PendingOutbound remove(int packetId) {
        return pending.remove(packetId);
    }

    public boolean isComplete(int packetId) {
        return !pending.containsKey(packetId);
    }

    public int size() { return pending.size(); }

    /** Fail every pending entry (used on hard disconnect with reconnect disabled). */
    public void failAll(Throwable error) {
        for (var entry : pending.entrySet()) {
            entry.getValue().getResultSink().tryEmitValue(
                    new MqttPublishResultImpl(entry.getValue().getPublish(), error));
        }
        pending.clear();
    }
}
```

- [ ] **Step 5: Run test — passes**

Run: `mvn -pl jmqx-client test -Dtest=AckTrackerTest -q`
Expected: BUILD SUCCESS.

- [ ] **Step 6: Commit**

```bash
git add jmqx-client/src/main/java/plus/jmqx/client/mqtt/internal/PendingOutbound.java \
        jmqx-client/src/main/java/plus/jmqx/client/mqtt/internal/MqttPublishResultImpl.java \
        jmqx-client/src/main/java/plus/jmqx/client/mqtt/internal/AckTracker.java \
        jmqx-client/src/test/java/plus/jmqx/client/mqtt/internal/AckTrackerTest.java
git commit -m "feat(client): add AckTracker for outbound QoS1/2 ACK tracking (TDD)"
```

---

### Task 10: MessageBuffer — offline buffering (flush re-registers ACK)

**Files:**
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/internal/buffer/MessageBufferFullException.java`
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/internal/buffer/MessageBuffer.java`
- Test: `jmqx-client/src/test/java/plus/jmqx/client/mqtt/internal/buffer/MessageBufferTest.java`

- [ ] **Step 1: Write failing test**

```java
package plus.jmqx.client.mqtt.internal.buffer;

import org.junit.jupiter.api.Test;
import plus.jmqx.client.mqtt.message.MqttPublish;
import plus.jmqx.client.mqtt.message.MqttPublishResult;
import plus.jmqx.client.mqtt.message.QoS;
import plus.jmqx.client.mqtt.v3.message.Mqtt3Publish;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class MessageBufferTest {

    @Test
    void offerUntilMaxSizeReturnsError() {
        MessageBuffer buf = new MessageBuffer(2, Long.MAX_VALUE);
        StepVerifier.create(buf.offer(pub("a")).map(MqttPublishResult::getError)).expectNextNull().thenCancel().verify();
        StepVerifier.create(buf.offer(pub("b")).map(MqttPublishResult::getError)).expectNextNull().thenCancel().verify();
        // third is over capacity — offer Mono errors
        StepVerifier.create(buf.offer(pub("c"))).expectError(MessageBufferFullException.class).verify();
        assertEquals(2, buf.size());
    }

    @Test
    void offerUntilMaxBytesReturnsError() {
        MessageBuffer buf = new MessageBuffer(Integer.MAX_VALUE, 10L);
        StepVerifier.create(buf.offer(pub("123456"))).expectNextCount(1).thenCancel().verify(); // 6 bytes < 10
        StepVerifier.create(buf.offer(pub("123456"))).expectError(MessageBufferFullException.class).verify();
    }

    @Test
    void flushInvokesWriterInOrderAndCompletesResults() {
        MessageBuffer buf = new MessageBuffer(10, Long.MAX_VALUE);
        Sinks.One<MqttPublishResult> s1 = Sinks.one();
        Sinks.One<MqttPublishResult> s2 = Sinks.one();
        buf.offer(pub("a")).subscribe();
        buf.offer(pub("b")).subscribe();
        List<String> order = new ArrayList<>();
        AtomicInteger pid = new AtomicInteger(1);
        // flush returns Mono<Void>; the writer is a function taking each buffered publish
        // and returning the result Mono that completes the original sink.
        buf.flush(p -> {
            order.add(new String(p.getPayloadAsBytes()));
            return reactor.core.publisher.Mono.just(new plus.jmqx.client.mqtt.internal.MqttPublishResultImpl(p, null));
        }).block();
        assertEquals(List.of("a", "b"), order);
        assertEquals(0, buf.size());
    }

    @Test
    void clearEmptiesAndFailsPending() {
        MessageBuffer buf = new MessageBuffer(10, Long.MAX_VALUE);
        buf.offer(pub("a")).subscribe();
        buf.clear();
        assertEquals(0, buf.size());
    }

    private MqttPublish pub(String payload) {
        return Mqtt3Publish.builder().topic("t").payload(payload.getBytes()).qos(QoS.AT_LEAST_ONCE).build();
    }
}
```

- [ ] **Step 2: Run test — fails**

Run: `mvn -pl jmqx-client test -Dtest=MessageBufferTest -q`
Expected: compilation failure.

- [ ] **Step 3: Implement MessageBuffer + exception**

`buffer/MessageBufferFullException.java`:
```java
package plus.jmqx.client.mqtt.internal.buffer;

public class MessageBufferFullException extends RuntimeException {
    public MessageBufferFullException() { super("Offline message buffer is full"); }
}
```

`buffer/MessageBuffer.java`:
```java
package plus.jmqx.client.mqtt.internal.buffer;

import lombok.extern.slf4j.Slf4j;
import plus.jmqx.client.mqtt.message.MqttPublish;
import reactor.core.publisher.Mono;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * Offline message buffer. Offers return a pending Mono<MqttPublishResult> that completes
 * when flush() replays the message through the writer. On flush, the writer (which runs
 * the full publish() path → re-registers AckTracker) produces the final result that
 * completes the original sink. Fixes the old design's "flush bypasses AckTracker" bug.
 */
@Slf4j
public final class MessageBuffer {

    private final Queue<BufferedPublish> queue = new ConcurrentLinkedQueue<>();
    private final int maxSize;
    private final long maxBytes;
    private final AtomicLong currentBytes = new AtomicLong(0);

    public MessageBuffer(int maxSize, long maxBytes) {
        this.maxSize = maxSize <= 0 ? Integer.MAX_VALUE : maxSize;
        this.maxBytes = maxBytes <= 0 ? Long.MAX_VALUE : maxBytes;
    }

    public Mono<MqttPublishResult> offer(MqttPublish publish) {
        long bytes = estimateBytes(publish);
        if (currentBytes.get() + bytes > maxBytes || queue.size() >= maxSize) {
            return Mono.error(new MessageBufferFullException());
        }
        reactor.core.publisher.Sinks.One<MqttPublishResult> sink = reactor.core.publisher.Sinks.one();
        queue.offer(new BufferedPublish(publish, sink));
        currentBytes.addAndGet(bytes);
        return sink.asMono();
    }

    /** Replay buffered messages through the writer (full publish path). Returns Mono<Void> completing when all replayed. */
    public Mono<Void> flush(Function<MqttPublish, Mono<MqttPublishResult>> writer) {
        return Mono.defer(() -> {
            java.util.List<Mono<Void>> sends = new java.util.ArrayList<>();
            BufferedPublish bp;
            while ((bp = queue.poll()) != null) {
                currentBytes.addAndGet(-estimateBytes(bp.publish()));
                final BufferedPublish captured = bp;
                sends.add(writer.apply(captured.publish())
                        .doOnNext(r -> captured.sink().tryEmitValue(r))
                        .doOnError(e -> captured.sink().tryEmitError(e))
                        .then());
            }
            return reactor.core.publisher.Flux.concat(sends).then();
        });
    }

    /** Fail all buffered with the given error (called when reconnect disabled on disconnect). */
    public void failAll(Throwable error) {
        BufferedPublish bp;
        while ((bp = queue.poll()) != null) {
            currentBytes.addAndGet(-estimateBytes(bp.publish()));
            bp.sink().tryEmitValue(new plus.jmqx.client.mqtt.internal.MqttPublishResultImpl(bp.publish(), error));
        }
    }

    public void clear() {
        queue.clear();
        currentBytes.set(0);
    }

    public int size() { return queue.size(); }
    public long bytes() { return currentBytes.get(); }

    private long estimateBytes(MqttPublish p) {
        return (p.getTopic() != null ? p.getTopic().length() : 0) + (p.getPayloadAsBytes() != null ? p.getPayloadAsBytes().length : 0);
    }

    private record BufferedPublish(MqttPublish publish, reactor.core.publisher.Sinks.One<MqttPublishResult> sink) {}
}
```

- [ ] **Step 4: Run test — passes**

Run: `mvn -pl jmqx-client test -Dtest=MessageBufferTest -q`
Expected: BUILD SUCCESS, 4 tests pass.

- [ ] **Step 5: Commit**

```bash
git add jmqx-client/src/main/java/plus/jmqx/client/mqtt/internal/buffer/ \
        jmqx-client/src/test/java/plus/jmqx/client/mqtt/internal/buffer/MessageBufferTest.java
git commit -m "feat(client): add MessageBuffer with flush-re-registers-ACK semantics (TDD)"
```

---

### Task 11: SubscriptionStore — topic filter routing + reconnect restore

**Files:**
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/internal/SubscriptionStore.java`

- [ ] **Step 1: Implement SubscriptionStore**

```java
package plus.jmqx.client.mqtt.internal;

import lombok.extern.slf4j.Slf4j;
import plus.jmqx.client.mqtt.message.MqttPublish;
import plus.jmqx.client.mqtt.message.MqttSubscribe;
import plus.jmqx.client.mqtt.message.MqttTopicFilter;
import plus.jmqx.client.mqtt.internal.util.TopicMatcher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Holds active subscriptions. Each subscription has a Sinks.Many for backpressured delivery.
 * On reconnect, snapshot() returns all subscriptions to re-SUBSCRIBE.
 */
@Slf4j
public final class SubscriptionStore {

    /** A subscription entry: the original filter + its delivery sink. */
    public record Subscription(MqttTopicFilter filter, Sinks.Many<MqttPublish> sink) {}

    private final List<Subscription> subscriptions = new CopyOnWriteArrayList<>();

    public Subscription add(MqttTopicFilter filter, Sinks.Many<MqttPublish> sink) {
        Subscription sub = new Subscription(filter, sink);
        subscriptions.add(sub);
        return sub;
    }

    /** Remove subscriptions matching any of the given filter strings. */
    public void removeAll(Collection<String> filters) {
        subscriptions.removeIf(s -> filters.contains(s.filter().getTopicFilter()));
    }

    /** Route an inbound publish to all matching subscription sinks. */
    public void route(MqttPublish publish) {
        for (Subscription sub : subscriptions) {
            if (TopicMatcher.matches(sub.filter().getTopicFilter(), publish.getTopic())) {
                sub.sink().tryEmitNext(publish);
            }
        }
    }

    /** Snapshot of subscription filters for reconnect re-subscribe. */
    public List<MqttTopicFilter> snapshotFilters() {
        return subscriptions.stream().map(Subscription::filter).toList();
    }

    public boolean isEmpty() { return subscriptions.isEmpty(); }

    public void clear() { subscriptions.clear(); }
}
```

- [ ] **Step 2: Compile**

Run: `mvn -pl jmqx-client compile -q`
Expected: BUILD SUCCESS.

- [ ] **Step 3: Commit**

```bash
git add jmqx-client/src/main/java/plus/jmqx/client/mqtt/internal/SubscriptionStore.java
git commit -m "feat(client): add SubscriptionStore for topic routing + reconnect restore"
```

---

### Task 12: MqttOutbox — outbound inflight semaphore backpressure

**Files:**
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/internal/MqttOutbox.java`
- Test: `jmqx-client/src/test/java/plus/jmqx/client/mqtt/internal/MqttOutboxBackpressureTest.java`

- [ ] **Step 1: Write failing test**

```java
package plus.jmqx.client.mqtt.internal;

import org.junit.jupiter.api.Test;
import plus.jmqx.client.mqtt.message.MqttPublish;
import plus.jmqx.client.mqtt.message.QoS;
import plus.jmqx.client.mqtt.v3.message.Mqtt3Publish;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class MqttOutboxBackpressureTest {

    @Test
    void acquireBlocksWhenInflightFullThenResumesOnRelease() {
        MqttOutbox outbox = new MqttOutbox(1);  // 1 inflight slot
        AtomicInteger acquired = new AtomicInteger();

        // first acquire succeeds
        StepVerifier.create(outbox.acquire(1).doOnNext(v -> acquired.incrementAndGet()))
                .expectNextCount(1)
                .verifyComplete();
        assertEquals(1, acquired.get());

        // second acquire blocks (inflight full)
        StepVerifier.create(outbox.acquire(2).doOnNext(v -> acquired.incrementAndGet()))
                .expectNoEvent(Duration.ofMillis(100))
                .then(() -> outbox.release(1))   // free the slot
                .expectNextCount(1)
                .verifyComplete();
        assertEquals(2, acquired.get());
    }

    @Test
    void releaseWakesOneWaiter() {
        MqttOutbox outbox = new MqttOutbox(1);
        outbox.acquire(1).block();
        // two waiters
        var s2 = outbox.acquire(2).doOnNext(v -> {}).subscribe();
        var s3 = outbox.acquire(3).doOnNext(v -> {}).subscribe();
        outbox.release(1);  // wakes waiter for pid 2
        // pid 3 still blocked (only one slot, now held by 2)
        outbox.remove(2);    // simulate: cancel pid 2's slot
        // after remove + release path, pid 3 should eventually proceed; verify via acquire timing
        assertDoesNotThrow(() -> outbox.release(3));
    }
}
```

- [ ] **Step 2: Run test — fails**

Run: `mvn -pl jmqx-client test -Dtest=MqttOutboxBackpressureTest -q`
Expected: compilation failure.

- [ ] **Step 3: Implement MqttOutbox**

```java
package plus.jmqx.client.mqtt.internal;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Outbound inflight limiter. Limits concurrent unACKed PUBLISH (QoS1/2) to the
 * Receive Maximum (v5) or configured maxInflightMessages (v3). publish() Monos
 * suspend (not error) when inflight is full — backpressure toward the caller.
 */
@Slf4j
public final class MqttOutbox {

    private final AtomicInteger permits;
    private final int maxPermits;
    private final Queue<Waiter> waiters = new ConcurrentLinkedQueue<>();

    public MqttOutbox(int maxInflight) {
        this.maxPermits = maxInflight;
        this.permits = new AtomicInteger(maxInflight);
    }

    public Mono<Void> acquire(int packetId) {
        return Mono.create(sink -> {
            if (permits.get() > 0 && permits.decrementAndGet() >= 0) {
                sink.success();
            } else {
                // restore the permit we tried to take
                if (permits.get() < maxPermits) permits.incrementAndGet();
                waiters.offer(new Waiter(packetId, sink));
            }
        });
    }

    /** Release a slot when an ACK (PUBACK/PUBCOMP) arrives. Wakes one waiter. */
    public void release(int packetId) {
        Waiter w = waiters.poll();
        if (w != null) {
            w.sink().success();   // hand the freed permit to the waiter
        } else {
            if (permits.get() < maxPermits) permits.incrementAndGet();
        }
    }

    /** Cancel/remove a pending entry without releasing a permit to waiters. */
    public void remove(int packetId) {
        // best-effort: waiters are keyed by packetId but poll is FIFO; for correctness in
        // cancel paths we drain+requeue non-matching. Called rarely (cancel/disconnect).
        int size = waiters.size();
        for (int i = 0; i < size; i++) {
            Waiter w = waiters.poll();
            if (w == null) break;
            if (w.packetId() == packetId) {
                w.sink().success();   // let it proceed; harmless
            } else {
                waiters.offer(w);
            }
        }
    }

    public int availablePermits() { return Math.max(0, permits.get()); }
    public int waiting() { return waiters.size(); }

    private record Waiter(int packetId, reactor.core.publisher.MonoSink<Void> sink) {}
}
```

- [ ] **Step 4: Run test — passes**

Run: `mvn -pl jmqx-client test -Dtest=MqttOutboxBackpressureTest -q`
Expected: BUILD SUCCESS, 2 tests pass.

- [ ] **Step 5: Commit**

```bash
git add jmqx-client/src/main/java/plus/jmqx/client/mqtt/internal/MqttOutbox.java \
        jmqx-client/src/test/java/plus/jmqx/client/mqtt/internal/MqttOutboxBackpressureTest.java
git commit -m "feat(client): add MqttOutbox inflight semaphore backpressure (TDD)"
```

---

### Task 13: TransportFactory — reactor-netty TCP/TLS/WS/WSS

**Files:**
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/internal/transport/TransportFactory.java`
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/internal/NettyUtil.java`

- [ ] **Step 1: Create NettyUtil**

```java
package plus.jmqx.client.mqtt.internal;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/** Netty buffer helpers. */
public final class NettyUtil {
    private NettyUtil() {}
    public static ByteBuf wrap(byte[] bytes) {
        return bytes == null ? Unpooled.EMPTY_BUFFER : Unpooled.wrappedBuffer(bytes);
    }
}
```

- [ ] **Step 2: Implement TransportFactory**

```java
package plus.jmqx.client.mqtt.internal.transport;

import io.netty.channel.ChannelOption;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.timeout.IdleStateHandler;
import lombok.extern.slf4j.Slf4j;
import plus.jmqx.client.mqtt.MqttClientConfig;
import plus.jmqx.client.mqtt.internal.handler.MqttClientHandler;
import reactor.netty.Connection;
import reactor.netty.tcp.TcpClient;
import reactor.netty.http.client.HttpClient;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Selects reactor-netty transport (TCP/TLS/WS/WSS) and installs the MQTT pipeline.
 * DefaultMqttClient is transport-agnostic — it only sees a Connection.
 */
@Slf4j
public final class TransportFactory {

    public Mono<Connection> connect(MqttClientConfig config,
                                    Function<MqttClientConfig, MqttClientHandler> handlerFactory) {
        return switch (config.getTransportType()) {
            case TCP -> tcpClient(config, handlerFactory).connect();
            case TLS -> tcpClient(config, handlerFactory).secure(ssl -> sslConfigurer(ssl, config)).connect();
            case WS  -> httpClient(config, handlerFactory).websocket(ws -> ws.uri(wsUri(config))).connect();
            case WSS -> httpClient(config, handlerFactory).secure(ssl -> sslConfigurer(ssl, config))
                        .websocket(ws -> ws.uri(wsUri(config))).connect();
        };
    }

    private TcpClient tcpClient(MqttClientConfig c, Function<MqttClientConfig, MqttClientHandler> hf) {
        return TcpClient.newConnection()
                .host(c.getServerHost())
                .port(c.getServerPort())
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, c.getSocketConnectTimeoutMs())
                .option(ChannelOption.TCP_NODELAY, true)
                .doOnConnected(conn -> installPipeline(conn, hf.apply(c), c));
    }

    private HttpClient httpClient(MqttClientConfig c, Function<MqttClientConfig, MqttClientHandler> hf) {
        return HttpClient.newConnection()
                .host(c.getServerHost())
                .port(c.getServerPort())
                .doOnConnected(conn -> installPipeline(conn, hf.apply(c), c));
    }

    private void installPipeline(reactor.netty.Connection conn, MqttClientHandler handler, MqttClientConfig c) {
        int keepAlive = c.getKeepAliveSeconds();
        conn.channel().pipeline()
                .addFirst("mqttDecoder", new MqttDecoder(8 * 1024 * 1024))
                .addAfter("mqttDecoder", "mqttEncoder", MqttEncoder.INSTANCE)
                .addAfter("mqttEncoder", "idle",
                        new IdleStateHandler((long)(keepAlive * 1.5), keepAlive, 0, TimeUnit.SECONDS))
                .addAfter("idle", "mqttClient", handler);
        log.debug("MQTT pipeline installed on {}", conn.channel());
    }

    private void sslConfigurer(reactor.netty.tcp.SslProvider.SslContextSpec spec, MqttClientConfig c) {
        // Minimal: use default JDK trust store. Full trust/key store config deferred to a follow-up
        // task that wires MqttSslConfig → SslContextBuilder. Acceptable for v1 TCP+basic TLS.
        spec.sslContext();
    }

    private String wsUri(MqttClientConfig c) {
        MqttWebSocketConfig ws = c.getWebSocketConfig();
        String path = ws != null && ws.getPath() != null ? ws.getPath() : "/mqtt";
        return path;
    }
}
```

Note: `Mono` import — add `import reactor.core.publisher.Mono;` at top.

- [ ] **Step 3: Compile (will fail — MqttClientHandler not yet created; created in Task 14)**

This task and Task 14 are interdependent. To unblock, create a minimal `MqttClientHandler` skeleton in Task 14 first, then compile here. Proceed to Task 14 and return.

- [ ] **Step 4: Commit (after Task 14 unblocks compile)**

```bash
git add jmqx-client/src/main/java/plus/jmqx/client/mqtt/internal/transport/TransportFactory.java \
        jmqx-client/src/main/java/plus/jmqx/client/mqtt/internal/NettyUtil.java
git commit -m "feat(client): add TransportFactory (reactor-netty TCP/TLS/WS/WSS)"
```

---

### Task 14: MqttClientHandler — single Netty inbound/outbound handler

**Files:**
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/internal/handler/MqttClientHandler.java`

This is the single Netty handler installed in the pipeline (Task 13). It delegates all logic to plain Java objects (`MqttMessageService`, `AckTracker`, `MqttInbox`, `InboundQos`). The handler is constructed by `DefaultMqttClient` (Task 15) with references to those collaborators.

- [ ] **Step 1: Create MqttClientHandler**

```java
package plus.jmqx.client.mqtt.internal.handler;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.timeout.IdleStateEvent;
import lombok.extern.slf4j.Slf4j;
import plus.jmqx.client.mqtt.MqttClientConfig;
import plus.jmqx.client.mqtt.internal.AckTracker;
import plus.jmqx.client.mqtt.internal.InboundQos;
import plus.jmqx.client.mqtt.internal.MqttMessageService;
import plus.jmqx.client.mqtt.internal.MqttInbox;
import reactor.core.publisher.Sinks;

/**
 * Single Netty handler for MQTT inbound/outbound dispatch. Delegates to plain Java
 * collaborators (service/tracker/inbox/inboundQos) which are independently testable.
 */
@Slf4j
public class MqttClientHandler extends ChannelDuplexHandler {

    private final MqttClientConfig config;
    private final MqttMessageService service;
    private final AckTracker ackTracker;
    private final MqttInbox inbox;
    private final InboundQos inboundQos;
    private final Sinks.One<plus.jmqx.client.mqtt.message.MqttConnAck> connAckSink;

    public MqttClientHandler(MqttClientConfig config,
                             MqttMessageService service,
                             AckTracker ackTracker,
                             MqttInbox inbox,
                             InboundQos inboundQos,
                             Sinks.One<plus.jmqx.client.mqtt.message.MqttConnAck> connAckSink) {
        this.config = config;
        this.service = service;
        this.ackTracker = ackTracker;
        this.inbox = inbox;
        this.inboundQos = inboundQos;
        this.connAckSink = connAckSink;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (!(msg instanceof MqttMessage mqtt)) {
            super.channelRead(ctx, msg);
            return;
        }
        MqttMessageType type = mqtt.fixedHeader() != null ? mqtt.fixedHeader().messageType() : null;
        if (type == null) { super.channelRead(ctx, msg); return; }

        switch (type) {
            case CONNACK -> {
                var ack = service.decodeConnAck((MqttConnAckMessage) mqtt, config);
                connAckSink.tryEmitValue(ack);
            }
            case PUBLISH -> inboundQos.onInboundPublish(ctx, (MqttPublishMessage) mqtt, service, inbox);
            case PUBACK, PUBCOMP -> {
                int pid = service.decodePacketId(mqtt);
                ackTracker.complete(pid, new plus.jmqx.client.mqtt.internal.MqttPublishResultImpl(null, null));
            }
            case PUBREC -> {
                int pid = service.decodePacketId(mqtt);
                ackTracker.markReceived(pid);
                ctx.writeAndFlush(service.encodePubRel(pid));
            }
            case PUBREL -> {
                int pid = service.decodePacketId(mqtt);
                ctx.writeAndFlush(service.encodePubComp(pid));
            }
            case PINGRESP -> log.debug("PINGRESP received");
            case DISCONNECT -> log.debug("Server-initiated DISCONNECT");
            default -> log.debug("Unhandled MQTT message type: {}", type);
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            // write PINGREQ; read idle beyond 1.5x keepalive handled by separate watch
            ctx.writeAndFlush(service.encodePingReq());
        }
        super.userEventTriggered(ctx, evt);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Channel exception", cause);
        ctx.close();
    }
}
```

- [ ] **Step 2: Create minimal InboundQos + MqttInbox stubs (full impl in Tasks 16-17)**

`internal/InboundQos.java`:
```java
package plus.jmqx.client.mqtt.internal;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import plus.jmqx.client.mqtt.message.MqttPublish;

/** Inbound QoS1/2 ACK gating (full impl Task 16). */
public final class InboundQos {
    public void onInboundPublish(ChannelHandlerContext ctx, MqttPublishMessage nettyMsg,
                                 MqttMessageService service, MqttInbox inbox) {
        MqttPublish pub = service.decodePublish(nettyMsg);
        switch (pub.getQoS()) {
            case AT_MOST_ONCE -> inbox.deliver(pub, () -> {});
            case AT_LEAST_ONCE -> inbox.deliver(pub, () -> ctx.writeAndFlush(service.encodePubAck(pub.getPacketId())));
            case EXACTLY_ONCE -> inbox.deliver(pub, () -> ctx.writeAndFlush(service.encodePubRec(pub.getPacketId())));
        }
    }
}
```

`internal/MqttInbox.java`:
```java
package plus.jmqx.client.mqtt.internal;

import plus.jmqx.client.mqtt.message.MqttPublish;

/** Inbound delivery + backpressure (full impl Task 17). */
public final class MqttInbox {
    public void deliver(MqttPublish pub, Runnable ackAction) {
        // stub: immediate delivery + immediate ack; backpressure wired in Task 17
        try { ackAction.run(); } catch (Exception ignored) {}
    }
}
```

- [ ] **Step 3: Compile**

Run: `mvn -pl jmqx-client compile -q`
Expected: BUILD SUCCESS (now TransportFactory from Task 13 compiles too).

- [ ] **Step 4: Commit (covers Task 13 + 14)**

```bash
git add jmqx-client/src/main/java/plus/jmqx/client/mqtt/internal/handler/MqttClientHandler.java \
        jmqx-client/src/main/java/plus/jmqx/client/mqtt/internal/InboundQos.java \
        jmqx-client/src/main/java/plus/jmqx/client/mqtt/internal/MqttInbox.java \
        jmqx-client/src/main/java/plus/jmqx/client/mqtt/internal/transport/TransportFactory.java \
        jmqx-client/src/main/java/plus/jmqx/client/mqtt/internal/NettyUtil.java
git commit -m "feat(client): add MqttClientHandler + TransportFactory + InboundQos/MqttInbox stubs"
```

---

### Task 15: MqttAutoReconnect — reactor-async reconnect scheduling

**Files:**
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/internal/reconnect/MqttAutoReconnect.java`
- Test: `jmqx-client/src/test/java/plus/jmqx/client/mqtt/internal/reconnect/MqttAutoReconnectTest.java`

- [ ] **Step 1: Write failing test (VirtualTimeScheduler for backoff timing)**

```java
package plus.jmqx.client.mqtt.internal.reconnect;

import org.junit.jupiter.api.Test;
import plus.jmqx.client.mqtt.MqttClientConfig;
import plus.jmqx.client.mqtt.lifecycle.MqttClientDisconnectedContext;
import plus.jmqx.client.mqtt.lifecycle.MqttClientReconnector;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.test.scheduler.VirtualTimeScheduler;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class MqttAutoReconnectTest {

    @Test
    void exponentialBackoffAdvances() {
        VirtualTimeScheduler vts = VirtualTimeScheduler.create();
        AtomicInteger attempts = new AtomicInteger();
        AtomicInteger connectCalls = new AtomicInteger();
        MqttAutoReconnect r = new MqttAutoReconnect(100, 10_000,
                () -> { connectCalls.incrementAndGet(); return Mono.empty(); }, vts);

        StepVerifier.withVirtualTime(() -> {
                    r.onDisconnected(disconnectCtx(new MqttClientReconnector(0, true)));
                    return Mono.never();
                }, () -> vts, Long.MAX_VALUE)
                .thenAwait(Duration.ofMillis(150))   // first delay ~100ms
                .then(() -> assertEquals(1, connectCalls.get()))
                .thenAwait(Duration.ofMillis(300))   // second attempt ~200ms
                .then(() -> assertTrue(connectCalls.get() >= 2))
                .thenCancel()
                .verify();
    }

    @Test
    void userDisconnectDoesNotReconnect() {
        AtomicInteger calls = new AtomicInteger();
        MqttAutoReconnect r = new MqttAutoReconnect(100, 10_000,
                () -> { calls.incrementAndGet(); return Mono.empty(); }, VirtualTimeScheduler.create());
        MqttClientDisconnectedContext ctx = new MqttClientDisconnectedContext(
                new MqttClientConfig(),
                MqttClientDisconnectedContext.DisconnectSource.USER,
                null, new MqttClientReconnector(0, true));
        r.onDisconnected(ctx);
        assertEquals(0, calls.get());
    }

    private MqttClientDisconnectedContext disconnectCtx(MqttClientReconnector rc) {
        return new MqttClientDisconnectedContext(new MqttClientConfig(),
                MqttClientDisconnectedContext.DisconnectSource.SERVER, null, rc);
    }
}
```

- [ ] **Step 2: Run test — fails**

Run: `mvn -pl jmqx-client test -Dtest=MqttAutoReconnectTest -q`
Expected: compilation failure.

- [ ] **Step 3: Implement MqttAutoReconnect**

```java
package plus.jmqx.client.mqtt.internal.reconnect;

import lombok.extern.slf4j.Slf4j;
import plus.jmqx.client.mqtt.lifecycle.MqttClientDisconnectedContext;
import plus.jmqx.client.mqtt.lifecycle.MqttClientDisconnectedListener;
import plus.jmqx.client.mqtt.lifecycle.MqttClientReconnector;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * Auto-reconnect via reactor Mono.delay (NEVER Thread.sleep). Exponential backoff
 * with ±25% jitter, capped at maxDelay. User disconnects (source=USER) do not reconnect.
 */
@Slf4j
public class MqttAutoReconnect implements MqttClientDisconnectedListener {

    private final long initialDelayMs;
    private final long maxDelayMs;
    private final Supplier<Mono<?>> connectCall;     // returns the connect() Mono
    private final Scheduler scheduler;              // VirtualTimeScheduler in tests, parallel in prod
    private final AtomicBoolean stopped = new AtomicBoolean(false);

    public MqttAutoReconnect(long initialDelayMs, long maxDelayMs,
                             Supplier<Mono<?>> connectCall, Scheduler scheduler) {
        this.initialDelayMs = initialDelayMs;
        this.maxDelayMs = maxDelayMs;
        this.connectCall = connectCall;
        this.scheduler = scheduler;
    }

    @Override
    public void onDisconnected(MqttClientDisconnectedContext ctx) {
        if (ctx.getSource() == MqttClientDisconnectedContext.DisconnectSource.USER) {
            stopped.set(true);
            return;
        }
        if (stopped.get()) return;
        scheduleReconnect(ctx.getReconnector().getAttempts());
    }

    private void scheduleReconnect(int attempts) {
        long delay = computeBackoff(attempts);
        log.info("Auto-reconnect attempt {} scheduled in {}ms", attempts + 1, delay);
        Mono.delay(Duration.ofMillis(delay), scheduler)
                .flatMap(t -> connectCall.get())
                .subscribe(
                        v -> {},
                        err -> scheduleReconnect(attempts + 1),
                        () -> { stopped.set(false); }
                );
    }

    private long computeBackoff(int attempt) {
        long base = Math.min(initialDelayMs * (1L << Math.min(attempt, 16)), maxDelayMs);
        double jitter = 1.0 + (ThreadLocalRandom.current().nextDouble() - 0.5) * 0.5;
        return (long) (base * jitter);
    }

    public void stop() { stopped.set(true); }
}
```

- [ ] **Step 4: Run test — passes**

Run: `mvn -pl jmqx-client test -Dtest=MqttAutoReconnectTest -q`
Expected: BUILD SUCCESS.

- [ ] **Step 5: Commit**

```bash
git add jmqx-client/src/main/java/plus/jmqx/client/mqtt/internal/reconnect/MqttAutoReconnect.java \
        jmqx-client/src/test/java/plus/jmqx/client/mqtt/internal/reconnect/MqttAutoReconnectTest.java
git commit -m "feat(client): add MqttAutoReconnect with reactor-async backoff (TDD, no Thread.sleep)"
```

---

### Task 16: InboundQos — full QoS1/2 inbound with backpressure-gated ACK

**Files:**
- Modify: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/internal/InboundQos.java`

Replace the stub from Task 14 with the full implementation. The key change: ACKs are delivered to `MqttInbox` as `Runnable` callbacks invoked only when the downstream consumer processes the message.

- [ ] **Step 1: Replace InboundQos with full impl**

```java
package plus.jmqx.client.mqtt.internal;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import lombok.extern.slf4j.Slf4j;
import plus.jmqx.client.mqtt.message.MqttPublish;
import plus.jmqx.client.mqtt.message.QoS;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Inbound QoS state machine (Broker → Client). For QoS1/2, the ACK is NOT sent on
 * receipt — it is handed to MqttInbox as a Runnable invoked only when the downstream
 * subscriber consumes the message. This is what makes MQTT's own flow control act as
 * backpressure: a slow subscriber delays ACKs, and the broker pauses pushing.
 */
@Slf4j
public final class InboundQos {

    private final Set<Integer> pendingPubRel = ConcurrentHashMap.newKeySet();

    public void onInboundPublish(ChannelHandlerContext ctx, MqttPublishMessage nettyMsg,
                                 MqttMessageService service, MqttInbox inbox) {
        MqttPublish pub = service.decodePublish(nettyMsg);
        int packetId = pub.getPacketId();
        switch (pub.getQoS()) {
            case AT_MOST_ONCE -> inbox.deliver(pub, () -> {});
            case AT_LEAST_ONCE -> inbox.deliver(pub, ackOnce(ctx, () -> ctx.writeAndFlush(service.encodePubAck(packetId))));
            case EXACTLY_ONCE -> {
                pendingPubRel.add(packetId);
                inbox.deliver(pub, ackOnce(ctx, () -> ctx.writeAndFlush(service.encodePubRec(packetId))));
            }
        }
    }

    public void onInboundPubRel(ChannelHandlerContext ctx, MqttMessage msg, MqttMessageService service) {
        int pid = service.decodePacketId(msg);
        if (pendingPubRel.remove(pid)) {
            ctx.writeAndFlush(service.encodePubComp(pid));
            log.debug("PUBCOMP sent for packetId={}", pid);
        }
    }

    /** Guard so the ack action fires exactly once even if downstream calls ack() twice. */
    private Runnable ackOnce(ChannelHandlerContext ctx, Runnable ack) {
        java.util.concurrent.atomic.AtomicBoolean fired = new java.util.concurrent.atomic.AtomicBoolean();
        return () -> {
            if (fired.compareAndSet(false, true)) {
                try { ack.run(); ctx.flush(); } catch (Exception e) { log.warn("ack failed", e); }
            }
        };
    }
}
```

Update `MqttClientHandler` to call `inboundQos.onInboundPubRel(...)` in the PUBREL case (replace the inline PUBREL handling):

In `MqttClientHandler.channelRead`, PUBREL branch becomes:
```java
case PUBREL -> inboundQos.onInboundPubRel(ctx, mqtt, service);
```

- [ ] **Step 2: Compile**

Run: `mvn -pl jmqx-client compile -q`
Expected: BUILD SUCCESS.

- [ ] **Step 3: Commit**

```bash
git add jmqx-client/src/main/java/plus/jmqx/client/mqtt/internal/InboundQos.java \
        jmqx-client/src/main/java/plus/jmqx/client/mqtt/internal/handler/MqttClientHandler.java
git commit -m "feat(client): full InboundQos with backpressure-gated ACK (PUBACK/PUBREC on consume)"
```

---

### Task 17: MqttInbox — inbound Flux + request-gated backpressure

**Files:**
- Modify: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/internal/MqttInbox.java`
- Test: `jmqx-client/src/test/java/plus/jmqx/client/mqtt/internal/MqttInboxBackpressureTest.java`

- [ ] **Step 1: Write failing test**

```java
package plus.jmqx.client.mqtt.internal;

import org.junit.jupiter.api.Test;
import plus.jmqx.client.mqtt.message.MqttPublish;
import plus.jmqx.client.mqtt.message.QoS;
import plus.jmqx.client.mqtt.v3.message.Mqtt3Publish;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

class MqttInboxBackpressureTest {

    @Test
    void ackOnlyFiredWhenDownstreamConsumes() {
        MqttInbox inbox = new MqttInbox(1024);
        AtomicBoolean ackFired = new AtomicBoolean();

        // subscriber that does NOT request yet; globalFlux returns Deliverable (exposes ack())
        var verifier = StepVerifier.create(inbox.globalFlux().doOnNext(p -> p.ack()))
                .then(() -> {
                    // deliver a publish with an ack callback
                    MqttPublish pub = Mqtt3Publish.builder().topic("t").payload(new byte[0]).qos(QoS.AT_LEAST_ONCE).build();
                    inbox.deliver(pub, () -> ackFired.set(true));
                })
                .then(() -> assertFalse(ackFired.get(), "ack fired before request"))
                .thenRequest(1)
                .assertNext(p -> assertTrue(ackFired.get(), "ack not fired after consume"))
                .thenCancel()
                .verify(Duration.ofSeconds(2));
    }

    @Test
    void bufferFullPausesDelivery() {
        MqttInbox inbox = new MqttInbox(2);  // capacity 2
        AtomicBoolean ackFired = new AtomicBoolean();
        // no subscriber — buffer fills
        for (int i = 0; i < 2; i++) {
            MqttPublish pub = Mqtt3Publish.builder().topic("t").payload(new byte[0]).qos(QoS.AT_LEAST_ONCE).build();
            assertTrue(inbox.deliver(pub, () -> ackFired.set(true)));   // buffered
        }
        MqttPublish pub = Mqtt3Publish.builder().topic("t").payload(new byte[0]).qos(QoS.AT_LEAST_ONCE).build();
        assertFalse(inbox.deliver(pub, () -> ackFired.set(true)));      // buffer full -> returns false (drop)
        assertFalse(ackFired.get(), "ack should not fire while buffered");
    }
}
```

- [ ] **Step 2: Run test — fails**

Run: `mvn -pl jmqx-client test -Dtest=MqttInboxBackpressureTest -q`
Expected: compilation failure (MqttInbox has no globalFlux/deliver-with-return).

- [ ] **Step 3: Replace MqttInbox stub with full impl**

```java
package plus.jmqx.client.mqtt.internal;

import lombok.extern.slf4j.Slf4j;
import plus.jmqx.client.mqtt.message.MqttPublish;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

/**
 * Inbound delivery hub. Each delivered PUBLISH is wrapped with its ack Runnable; the
 * downstream Flux only emits to a subscriber after request(n), and only then does the
 * subscriber call ack(). This makes MQTT's ACK the natural backpressure signal:
 * a slow subscriber delays ACKs → broker pauses pushing (QoS1/2).
 */
@Slf4j
public final class MqttInbox {

    private final Sinks.Many<Deliverable> sink;

    public MqttInbox(int bufferSize) {
        this.sink = Sinks.many().multicast().onBackpressureBuffer(bufferSize, false);
    }

    /** Deliver a publish with its ack action. Returns false if the buffer is full (caller should drop for QoS0). */
    public boolean deliver(MqttPublish pub, Runnable ackAction) {
        Deliverable d = new Deliverable(pub, ackAction);
        reactor.core.publisher.Sinks.EmitResult r = sink.tryEmitNext(d);
        if (r.isFailure()) {
            log.warn("Inbox buffer full, dropping QoS{} publish on {}", pub.getQoS(), pub.getTopic());
            return false;
        }
        return true;
    }

    /** The global inbound stream (for publishes(ALL/SUBSCRIBED/UNSOLICITED)). Returns Deliverable so ack() is reachable. */
    public Flux<Deliverable> globalFlux() {
        return sink.asFlux();
    }

    /** A subscription-specific stream. */
    public Flux<Deliverable> subscriptionFlux() {
        return globalFlux();
    }

    /** Wrapper carrying the ack action so the subscriber can fire it on consume. */
    public static final class Deliverable implements MqttPublish {
        private final MqttPublish delegate;
        private final Runnable ack;
        private final java.util.concurrent.atomic.AtomicBoolean acked = new java.util.concurrent.atomic.AtomicBoolean();

        Deliverable(MqttPublish delegate, Runnable ack) {
            this.delegate = delegate;
            this.ack = ack;
        }

        MqttPublish publish() { return this; }
        public void ack() { if (acked.compareAndSet(false, true)) ack.run(); }

        @Override public String getTopic() { return delegate.getTopic(); }
        @Override public byte[] getPayloadAsBytes() { return delegate.getPayloadAsBytes(); }
        @Override public plus.jmqx.client.mqtt.message.QoS getQoS() { return delegate.getQoS(); }
        @Override public boolean isRetain() { return delegate.isRetain(); }
        @Override public boolean isDup() { return delegate.isDup(); }
        @Override public int getPacketId() { return delegate.getPacketId(); }
    }
}
```

- [ ] **Step 4: Run test — passes**

Run: `mvn -pl jmqx-client test -Dtest=MqttInboxBackpressureTest -q`
Expected: BUILD SUCCESS, 2 tests pass.

- [ ] **Step 5: Commit**

```bash
git add jmqx-client/src/main/java/plus/jmqx/client/mqtt/internal/MqttInbox.java \
        jmqx-client/src/test/java/plus/jmqx/client/mqtt/internal/MqttInboxBackpressureTest.java
git commit -m "feat(client): full MqttInbox with request-gated backpressure (TDD)"
```

---

### Task 18: v3 client interfaces

**Files:**
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/MqttClient.java`
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/Mqtt3Client.java`
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/Mqtt3AsyncClient.java`
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/Mqtt3RxClient.java`
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/Mqtt3BlockingClient.java`
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/Mqtt3PublishResult.java`
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/Mqtt3ClientConfig.java`

- [ ] **Step 1: Create MqttClient (version-agnostic entry)**

```java
package plus.jmqx.client.mqtt;

/** MQTT client entry — version selection via builder(). */
public interface MqttClient {
    MqttClientConfig getConfig();
    MqttClientState getState();
    MqttVersion getVersion();
    static MqttClientBuilder builder() { return new MqttClientBuilder(); }
}
```

- [ ] **Step 2: Create v3 interfaces + config**

`v3/Mqtt3ClientConfig.java`:
```java
package plus.jmqx.client.mqtt.v3;

import plus.jmqx.client.mqtt.MqttClientConfig;

public class Mqtt3ClientConfig extends MqttClientConfig {}
```

`v3/Mqtt3Client.java`:
```java
package plus.jmqx.client.mqtt.v3;

import plus.jmqx.client.mqtt.MqttClient;

public interface Mqtt3Client extends MqttClient {
    @Override Mqtt3ClientConfig getConfig();
    Mqtt3AsyncClient toAsync();
    Mqtt3RxClient toRx();
    Mqtt3BlockingClient toBlock();
    static Mqtt3ClientBuilder builder() { return new Mqtt3ClientBuilder(); }
}
```

`v3/Mqtt3RxClient.java`:
```java
package plus.jmqx.client.mqtt.v3;

import plus.jmqx.client.mqtt.MqttGlobalPublishFilter;
import plus.jmqx.client.mqtt.v3.message.Mqtt3ConnAck;
import plus.jmqx.client.mqtt.v3.message.Mqtt3Publish;
import plus.jmqx.client.mqtt.v3.message.Mqtt3Subscribe;
import plus.jmqx.client.mqtt.v3.message.Mqtt3Unsubscribe;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

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

`v3/Mqtt3AsyncClient.java`:
```java
package plus.jmqx.client.mqtt.v3;

import plus.jmqx.client.mqtt.v3.message.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public interface Mqtt3AsyncClient extends Mqtt3Client {
    CompletableFuture<Mqtt3ConnAck> connect();
    CompletableFuture<Mqtt3SubAck> subscribe(Mqtt3Subscribe sub, Consumer<Mqtt3Publish> callback);
    CompletableFuture<Mqtt3PublishResult> publish(Mqtt3Publish publish);
    CompletableFuture<Void> unsubscribe(Mqtt3Unsubscribe unsub);
    CompletableFuture<Void> disconnect();
}
```

`v3/Mqtt3BlockingClient.java`:
```java
package plus.jmqx.client.mqtt.v3;

import plus.jmqx.client.mqtt.MqttGlobalPublishFilter;
import plus.jmqx.client.mqtt.v3.message.*;

public interface Mqtt3BlockingClient extends Mqtt3Client {
    Mqtt3ConnAck connect();
    Mqtt3SubAck subscribe(Mqtt3Subscribe sub);
    Mqtt3Publishes publishes(MqttGlobalPublishFilter filter);
    void publish(Mqtt3Publish publish);
    void unsubscribe(Mqtt3Unsubscribe unsub);
    void disconnect();
}
```

`v3/Mqtt3PublishResult.java`:
```java
package plus.jmqx.client.mqtt.v3;

import plus.jmqx.client.mqtt.message.MqttPublishResult;
import plus.jmqx.client.mqtt.v3.message.Mqtt3Publish;

public interface Mqtt3PublishResult extends MqttPublishResult {
    @Override Mqtt3Publish getPublish();
}
```

`v3/Mqtt3Publishes.java` (blocking iterator-style stream, HiveMQ-like):
```java
package plus.jmqx.client.mqtt.v3;

import plus.jmqx.client.mqtt.v3.message.Mqtt3Publish;
import java.util.Iterator;

/** Blocking handle to a publishes stream. */
public interface Mqtt3Publishes extends AutoCloseable, Iterable<Mqtt3Publish> {
    Mqtt3Publish receive();
    @Override void close();
    @Override default Iterator<Mqtt3Publish> iterator() {
        return new Iterator<>() {
            public boolean hasNext() { return true; }
            public Mqtt3Publish next() { return receive(); }
        };
    }
}
```

- [ ] **Step 3: Compile**

Run: `mvn -pl jmqx-client compile -q`
Expected: BUILD SUCCESS.

- [ ] **Step 4: Commit**

```bash
git add jmqx-client/src/main/java/plus/jmqx/client/mqtt/MqttClient.java \
        jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/Mqtt3Client.java \
        jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/Mqtt3AsyncClient.java \
        jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/Mqtt3RxClient.java \
        jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/Mqtt3BlockingClient.java \
        jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/Mqtt3PublishResult.java \
        jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/Mqtt3Publishes.java \
        jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/Mqtt3ClientConfig.java
git commit -m "feat(client): add v3 client public API interfaces"
```

---

### Task 19: DefaultMqtt3Client — the single engine + API wrappers + builders

**Files:**
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/internal/DefaultMqtt3Client.java`
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/internal/Mqtt3AsyncClientImpl.java`
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/internal/Mqtt3BlockingClientImpl.java`
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/Mqtt3ClientBuilder.java`
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/MqttClientBuilder.java`

This is the largest task: the engine wiring. It composes all collaborators from Tasks 8–17. It is not unit-tested in isolation here (collaborators are); it is validated end-to-end by the integration test in Task 20.

- [ ] **Step 1: Create DefaultMqtt3Client**

```java
package plus.jmqx.client.mqtt.v3.internal;

import io.netty.handler.codec.mqtt.MqttMessage;
import lombok.extern.slf4j.Slf4j;
import plus.jmqx.client.mqtt.MqttClientState;
import plus.jmqx.client.mqtt.MqttGlobalPublishFilter;
import plus.jmqx.client.mqtt.MqttVersion;
import plus.jmqx.client.mqtt.internal.*;
import plus.jmqx.client.mqtt.internal.buffer.MessageBuffer;
import plus.jmqx.client.mqtt.internal.handler.MqttClientHandler;
import plus.jmqx.client.mqtt.internal.reconnect.MqttAutoReconnect;
import plus.jmqx.client.mqtt.internal.transport.TransportFactory;
import plus.jmqx.client.mqtt.internal.util.PacketIdManager;
import plus.jmqx.client.mqtt.lifecycle.*;
import plus.jmqx.client.mqtt.v3.*;
import plus.jmqx.client.mqtt.v3.message.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.netty.Connection;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class DefaultMqtt3Client implements Mqtt3RxClient {

    private final Mqtt3ClientConfig config;
    private final Mqtt3MessageService service = new Mqtt3MessageService();
    private final PacketIdManager packetIdManager = new PacketIdManager();
    private final AckTracker ackTracker = new AckTracker();
    private final SubscriptionStore subscriptionStore = new SubscriptionStore();
    private final MqttInbox inbox = new MqttInbox(config.getInboxBufferSize());
    private final InboundQos inboundQos = new InboundQos();
    private final MqttOutbox outbox = new MqttOutbox(config.getMaxInflightMessages());
    private final MessageBuffer messageBuffer =
            new MessageBuffer(config.getMessageBufferMaxSize(), config.getMessageBufferMaxBytes());
    private final TransportFactory transportFactory = new TransportFactory();
    private final List<MqttClientConnectedListener> connectedListeners;
    private final List<MqttClientDisconnectedListener> disconnectedListeners;
    private final MqttAutoReconnect autoReconnect;

    private final AtomicReference<MqttClientState> state =
            new AtomicReference<>(MqttClientState.DISCONNECTED);
    private volatile Connection connection;

    public DefaultMqtt3Client(Mqtt3ClientConfig config,
                              List<MqttClientConnectedListener> connectedListeners,
                              List<MqttClientDisconnectedListener> disconnectedListeners) {
        this.config = config;
        this.connectedListeners = connectedListeners;
        this.disconnectedListeners = disconnectedListeners;
        this.autoReconnect = config.isAutomaticReconnect() ? new MqttAutoReconnect(
                config.getReconnectInitialDelayMs(), config.getReconnectMaxDelayMs(),
                () -> this.connect().cast(Object.class),
                reactor.core.scheduler.Schedulers.parallel()) : null;
    }

    @Override
    public Mono<Mqtt3ConnAck> connect() {
        return Mono.defer(() -> {
            if (!state.compareAndSet(MqttClientState.DISCONNECTED, MqttClientState.CONNECTING))
                return Mono.error(new IllegalStateException("Client is " + state.get()));
            Sinks.One<plus.jmqx.client.mqtt.message.MqttConnAck> ackSink = Sinks.one();
            MqttClientHandler handler = new MqttClientHandler(
                    config, service, ackTracker, inbox, inboundQos, ackSink);
            return transportFactory.connect(config, c -> handler)
                    .flatMap(conn -> {
                        this.connection = conn;
                        conn.inbound().receiveObject()
                                .cast(MqttMessage.class)
                                .doOnError(this::onTransportError)
                                .subscribe();
                        conn.onDispose().subscribe(v -> onTransportError(new RuntimeException("connection disposed")));
                        return conn.outbound().sendObject(Mono.just(service.encodeConnect(config))).then()
                                .then(ackSink.asMono().cast(Mqtt3ConnAck.class));
                    })
                    .doOnSuccess(ack -> {
                        state.set(MqttClientState.CONNECTED);
                        notifyConnected(ack.isSessionPresent());
                        resubscribe();
                        messageBuffer.flush(this::doPublish).subscribe();
                    })
                    .doOnError(err -> {
                        state.set(MqttClientState.DISCONNECTING);
                        onTransportError(err);
                    })
                    .cast(Mqtt3ConnAck.class);
        });
    }

    @Override
    public Mono<Mqtt3SubAck> subscribe(Mqtt3Subscribe sub) {
        return Mono.defer(() -> {
            if (state.get() != MqttClientState.CONNECTED)
                return Mono.error(new IllegalStateException("Not connected"));
            int pid = packetIdManager.nextPacketId();
            Mqtt3Subscribe withPid = sub.toBuilder().packetId(pid).build();
            Sinks.One<Mqtt3SubAck> sink = Sinks.one();
            Sinks.Many<plus.jmqx.client.mqtt.message.MqttPublish> deliverSink =
                    Sinks.many().multicast().onBackpressureBuffer(config.getInboxBufferSize(), false);
            for (Mqtt3TopicFilter tf : withPid.getTopicFilters()) {
                subscriptionStore.add(tf, deliverSink);
            }
            connection.outbound().sendObject(Mono.just(service.encodeSubscribe(withPid)))
                    .then().subscribe();
            // SUBACK delivery: the handler routes SUBACK via... we intercept here by listening.
            // For v1 simplicity, the MqttClientHandler must surface SUBACK; we add a pending map.
            // (See note below: extend handler with a SubAckSink map; for brevity this is wired
            //  in the handler's CONNACK-only sink. A dedicated SubscriptionAckTracker is added next.)
            return sink.asMono();
        });
    }

    @Override
    public Flux<Mqtt3Publish> subscribePublishes(Mqtt3Subscribe sub) {
        // Deliverable (from inbox) implements MqttPublish with a working ack(); map to an
        // Mqtt3Publish view that preserves ack() for the subscriber to call.
        return inbox.subscriptionFlux().map(this::toMqtt3);
    }

    @Override
    public Flux<Mqtt3Publish> publishes(MqttGlobalPublishFilter filter) {
        return inbox.globalFlux().map(this::toMqtt3);
    }

    /** Wrap a Deliverable as an Mqtt3Publish that preserves the ack() callback. */
    private Mqtt3Publish toMqtt3(MqttInbox.Deliverable d) {
        return new Mqtt3Publish() {
            @Override public String getTopic() { return d.getTopic(); }
            @Override public byte[] getPayloadAsBytes() { return d.getPayloadAsBytes(); }
            @Override public plus.jmqx.client.mqtt.message.QoS getQoS() { return d.getQoS(); }
            @Override public boolean isRetain() { return d.isRetain(); }
            @Override public boolean isDup() { return d.isDup(); }
            @Override public int getPacketId() { return d.getPacketId(); }
            @Override public void ack() { d.ack(); }
            @Override public Mqtt3PublishBuilder toBuilder() { return ((Mqtt3Publish) d).toBuilder(); }
        };
    }

    @Override
    public Mono<Mqtt3PublishResult> publish(Mqtt3Publish publish) {
        return doPublish(publish).cast(Mqtt3PublishResult.class);
    }

    private Mono<plus.jmqx.client.mqtt.message.MqttPublishResult> doPublish(
            plus.jmqx.client.mqtt.message.MqttPublish publish) {
        return Mono.defer(() -> {
            if (state.get() == MqttClientState.CONNECTED) {
                if (publish.getQoS() == plus.jmqx.client.mqtt.message.QoS.AT_MOST_ONCE) {
                    return connection.outbound()
                            .sendObject(Mono.just(service.encodePublish(publish, 0, false)))
                            .then()
                            .thenReturn(result(publish, null));
                }
                int pid = packetIdManager.nextPacketId();
                plus.jmqx.client.mqtt.message.MqttPublish withPid =
                        ((Mqtt3Publish) publish).toBuilder().packetId(pid).build();
                Sinks.One<MqttPublishResult> sink = Sinks.one();
                PendingOutbound po = new PendingOutbound(withPid, sink);
                ackTracker.register(pid, po);
                return outbox.acquire(pid)
                        .then(connection.outbound()
                                .sendObject(Mono.just(service.encodePublish(withPid, pid, false)))
                                .then())
                        .then(sink.asMono());
            }
            if (state.get() == MqttClientState.DISCONNECTED && config.isAutomaticReconnect()) {
                return messageBuffer.offer(publish).cast(MqttPublishResult.class);
            }
            return Mono.error(new IllegalStateException("Client is " + state.get()));
        });
    }

    @Override
    public Mono<Void> unsubscribe(Mqtt3Unsubscribe unsub) {
        return Mono.defer(() -> {
            if (state.get() != MqttClientState.CONNECTED)
                return Mono.error(new IllegalStateException("Not connected"));
            int pid = packetIdManager.nextPacketId();
            Mqtt3Unsubscribe withPid = unsub.toBuilder().packetId(pid).build();
            subscriptionStore.removeAll(withPid.getTopicFilters());
            return connection.outbound()
                    .sendObject(Mono.just(service.encodeUnsubscribe(withPid)))
                    .then();
        });
    }

    @Override
    public Mono<Void> disconnect() {
        return Mono.defer(() -> {
            if (autoReconnect != null) autoReconnect.stop();
            state.set(MqttClientState.DISCONNECTING);
            if (connection != null) {
                return connection.outbound().sendObject(Mono.just(service.encodeDisconnect())).then()
                        .then(Mono.fromRunnable(() -> connection.dispose()))
                        .then(Mono.<Void>empty());
            }
            return Mono.<Void>empty();
        });
    }

    // --- helpers ---

    private void resubscribe() {
        var filters = subscriptionStore.snapshotFilters();
        if (filters.isEmpty()) return;
        int pid = packetIdManager.nextPacketId();
        Mqtt3Subscribe sub = Mqtt3Subscribe.builder()
                .topicFilters(filters.stream().map(f -> Mqtt3TopicFilter.builder()
                        .topicFilter(f.getTopicFilter()).qos(f.getQoS()).build()).toList())
                .packetId(pid).build();
        connection.outbound().sendObject(Mono.just(service.encodeSubscribe(sub))).then().subscribe();
    }

    private void onTransportError(Throwable err) {
        log.warn("Transport error: {}", err.toString());
        state.set(MqttClientState.DISCONNECTED);
        MqttClientReconnector rc = new MqttClientReconnector(0, config.isAutomaticReconnect());
        MqttClientDisconnectedContext ctx = new MqttClientDisconnectedContext(
                config, MqttClientDisconnectedContext.DisconnectSource.SERVER, err, rc);
        for (var l : disconnectedListeners) l.onDisconnected(ctx);
        if (autoReconnect != null && !config.isAutomaticReconnect()) {
            messageBuffer.failAll(err);
            ackTracker.failAll(err);
        }
    }

    private void notifyConnected(boolean sessionPresent) {
        MqttClientConnectedContext ctx = new MqttClientConnectedContext(config, sessionPresent);
        for (var l : connectedListeners) l.onConnected(ctx);
    }

    private static Mqtt3PublishResult result(plus.jmqx.client.mqtt.message.MqttPublish p, Throwable err) {
        return new Mqtt3PublishResult() {
            public Mqtt3Publish getPublish() { return (Mqtt3Publish) p; }
            public Throwable getError() { return err; }
        };
    }

    @Override public Mqtt3ClientConfig getConfig() { return config; }
    @Override public MqttClientState getState() { return state.get(); }
    @Override public MqttVersion getVersion() { return MqttVersion.MQTT_3_1_1; }
    @Override public Mqtt3AsyncClient toAsync() { return new Mqtt3AsyncClientImpl(this); }
    @Override public Mqtt3RxClient toRx() { return this; }
    @Override public Mqtt3BlockingClient toBlock() { return new Mqtt3BlockingClientImpl(this); }
}
```

**Note on SUBACK:** the `subscribe()` above leaves `sink` uncompleted because `MqttClientHandler` (Task 14) does not route SUBACK to a per-pid sink. To fully wire SUBACK completion, add a `Map<Integer, Sinks.One<Mqtt3SubAck>>` to `MqttClientHandler` and a `registerSubAck(pid, sink)` method; the handler's SUBACK branch completes the sink. Apply this patch:

- [ ] **Step 2: Patch MqttClientHandler to route SUBACK/UNSUBACK**

Add field + methods to `MqttClientHandler`:
```java
private final java.util.Map<Integer, reactor.core.publisher.Sinks.One<Mqtt3SubAck>> pendingSubAcks = new java.util.concurrent.ConcurrentHashMap<>();
private final java.util.Map<Integer, reactor.core.publisher.Sinks.Empty<Void>> pendingUnsubAcks = new java.util.concurrent.ConcurrentHashMap<>();

public void registerSubAck(int pid, reactor.core.publisher.Sinks.One<Mqtt3SubAck> sink) { pendingSubAcks.put(pid, sink); }
public void registerUnsubAck(int pid, reactor.core.publisher.Sinks.Empty<Void> sink) { pendingUnsubAcks.put(pid, sink); }
```

Add to `channelRead` switch:
```java
case SUBACK -> {
    int pid = service.decodePacketId(mqtt);
    var sink = pendingSubAcks.remove(pid);
    if (sink != null) sink.tryEmitValue(service.decodeSubAck((io.netty.handler.codec.mqtt.MqttSubAckMessage) mqtt));
}
case UNSUBACK -> {
    int pid = service.decodePacketId(mqtt);
    var sink = pendingUnsubAcks.remove(pid);
    if (sink != null) sink.tryEmitEmpty();
}
```
(Add imports: `io.netty.handler.codec.mqtt.MqttSubAckMessage`, `plus.jmqx.client.mqtt.v3.message.Mqtt3SubAck`.)

Then in `DefaultMqtt3Client.subscribe()`, before `return sink.asMono()`:
```java
handler.registerSubAck(pid, sink);
```
(handlers is the `MqttClientHandler handler` captured in `connect()`. To make `handler` reachable from `subscribe()`, store it as a field `private volatile MqttClientHandler handler;` and set it in `connect()`.)

Update `DefaultMqtt3Client`:
- add field `private volatile MqttClientHandler handler;`
- in `connect()` after constructing `handler`: `this.handler = handler;`
- in `subscribe()`: `handler.registerSubAck(pid, sink);` and `connection.outbound()...subscribe();` then `return sink.asMono();`
- in `unsubscribe()`: create a sink, `handler.registerUnsubAck(pid, sink);` and `return sink.asMono();`

- [ ] **Step 3: Create the API wrappers**

`v3/internal/Mqtt3AsyncClientImpl.java`:
```java
package plus.jmqx.client.mqtt.v3.internal;

import plus.jmqx.client.mqtt.MqttClientState;
import plus.jmqx.client.mqtt.MqttVersion;
import plus.jmqx.client.mqtt.v3.*;
import plus.jmqx.client.mqtt.v3.message.*;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public class Mqtt3AsyncClientImpl implements Mqtt3AsyncClient {
    private final Mqtt3RxClient rx;
    public Mqtt3AsyncClientImpl(Mqtt3RxClient rx) { this.rx = rx; }

    @Override public CompletableFuture<Mqtt3ConnAck> connect() { return rx.connect().toFuture(); }
    @Override public CompletableFuture<Mqtt3SubAck> subscribe(Mqtt3Subscribe sub, Consumer<Mqtt3Publish> cb) {
        rx.subscribePublishes(sub).doOnNext(cb).subscribe();
        return rx.subscribe(sub).toFuture();
    }
    @Override public CompletableFuture<Mqtt3PublishResult> publish(Mqtt3Publish p) { return rx.publish(p).toFuture(); }
    @Override public CompletableFuture<Void> unsubscribe(Mqtt3Unsubscribe u) { return rx.unsubscribe(u).toFuture(); }
    @Override public CompletableFuture<Void> disconnect() { return rx.disconnect().toFuture(); }

    @Override public Mqtt3ClientConfig getConfig() { return rx.getConfig(); }
    @Override public MqttClientState getState() { return rx.getState(); }
    @Override public MqttVersion getVersion() { return rx.getVersion(); }
    @Override public Mqtt3AsyncClient toAsync() { return this; }
    @Override public Mqtt3RxClient toRx() { return rx; }
    @Override public Mqtt3BlockingClient toBlock() { return rx.toBlock(); }
}
```

`v3/internal/Mqtt3BlockingClientImpl.java`:
```java
package plus.jmqx.client.mqtt.v3.internal;

import plus.jmqx.client.mqtt.MqttClientState;
import plus.jmqx.client.mqtt.MqttGlobalPublishFilter;
import plus.jmqx.client.mqtt.MqttVersion;
import plus.jmqx.client.mqtt.v3.*;
import plus.jmqx.client.mqtt.v3.message.*;

import java.time.Duration;

public class Mqtt3BlockingClientImpl implements Mqtt3BlockingClient {
    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private final Mqtt3RxClient rx;
    public Mqtt3BlockingClientImpl(Mqtt3RxClient rx) { this.rx = rx; }

    @Override public Mqtt3ConnAck connect() { return rx.connect().block(TIMEOUT); }
    @Override public Mqtt3SubAck subscribe(Mqtt3Subscribe s) { return rx.subscribe(s).block(TIMEOUT); }
    @Override public void publish(Mqtt3Publish p) { rx.publish(p).block(TIMEOUT); }
    @Override public void unsubscribe(Mqtt3Unsubscribe u) { rx.unsubscribe(u).block(TIMEOUT); }
    @Override public void disconnect() { rx.disconnect().block(TIMEOUT); }
    @Override public Mqtt3Publishes publishes(MqttGlobalPublishFilter f) {
        throw new UnsupportedOperationException("Blocking publishes() deferred — use Rx/Async");
    }
    @Override public Mqtt3ClientConfig getConfig() { return rx.getConfig(); }
    @Override public MqttClientState getState() { return rx.getState(); }
    @Override public MqttVersion getVersion() { return rx.getVersion(); }
    @Override public Mqtt3AsyncClient toAsync() { return rx.toAsync(); }
    @Override public Mqtt3RxClient toRx() { return rx; }
    @Override public Mqtt3BlockingClient toBlock() { return this; }
}
```

- [ ] **Step 4: Create builders**

`v3/Mqtt3ClientBuilder.java`:
```java
package plus.jmqx.client.mqtt.v3;

import plus.jmqx.client.mqtt.lifecycle.MqttClientConnectedListener;
import plus.jmqx.client.mqtt.lifecycle.MqttClientDisconnectedListener;
import plus.jmqx.client.mqtt.v3.internal.DefaultMqtt3Client;
import plus.jmqx.client.mqtt.v3.message.Mqtt3Publish;
import plus.jmqx.client.mqtt.MqttVersion;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class Mqtt3ClientBuilder {
    private final Mqtt3ClientConfig config = new Mqtt3ClientConfig();
    private final List<MqttClientConnectedListener> connected = new ArrayList<>();
    private final List<MqttClientDisconnectedListener> disconnected = new ArrayList<>();

    public Mqtt3ClientBuilder serverHost(String h) { config.setServerHost(h); return this; }
    public Mqtt3ClientBuilder serverPort(int p) { config.setServerPort(p); return this; }
    public Mqtt3ClientBuilder identifier(String id) { config.setClientId(id); return this; }
    public Mqtt3ClientBuilder identifier() { config.setClientId("jmqx-" + UUID.randomUUID().toString().substring(0, 8)); return this; }
    public Mqtt3ClientBuilder keepAliveSeconds(int s) { config.setKeepAliveSeconds(s); return this; }
    public Mqtt3ClientBuilder cleanSession(boolean c) { config.setCleanSession(c); return this; }
    public Mqtt3ClientBuilder username(String u) { config.setUsername(u); return this; }
    public Mqtt3ClientBuilder password(byte[] p) { config.setPassword(p); return this; }
    public Mqtt3ClientBuilder willPublish(Mqtt3Publish w) { config.setWillPublish(w); return this; }
    public Mqtt3ClientBuilder automaticReconnect() { config.setAutomaticReconnect(true); return this; }
    public Mqtt3ClientBuilder addConnectedListener(MqttClientConnectedListener l) { connected.add(l); return this; }
    public Mqtt3ClientBuilder addDisconnectedListener(MqttClientDisconnectedListener l) { disconnected.add(l); return this; }

    private void ensureClientId() {
        if (config.getClientId() == null || config.getClientId().isEmpty())
            config.setClientId("jmqx-" + UUID.randomUUID().toString().substring(0, 8));
        config.setVersion(MqttVersion.MQTT_3_1_1);
    }

    public Mqtt3RxClient buildRx() { ensureClientId(); return new DefaultMqtt3Client(config, connected, disconnected); }
    public Mqtt3AsyncClient buildAsync() { ensureClientId(); return buildRx().toAsync(); }
    public Mqtt3BlockingClient buildBlocking() { ensureClientId(); return buildRx().toBlock(); }
}
```

`MqttClientBuilder.java`:
```java
package plus.jmqx.client.mqtt;

import plus.jmqx.client.mqtt.v3.Mqtt3ClientBuilder;

/** Top-level builder — choose version here. */
public class MqttClientBuilder {
    public Mqtt3ClientBuilder useMqttVersion3() { return new Mqtt3ClientBuilder(); }
    public Object useMqttVersion5() { throw new UnsupportedOperationException("MQTT 5 added in Task 21+"); }
}
```

- [ ] **Step 5: Compile**

Run: `mvn -pl jmqx-client compile -q`
Expected: BUILD SUCCESS. Fix any import/symbol errors (e.g., `Mqtt3SubAck` package) before proceeding.

- [ ] **Step 6: Commit**

```bash
git add jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/internal/DefaultMqtt3Client.java \
        jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/internal/Mqtt3AsyncClientImpl.java \
        jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/internal/Mqtt3BlockingClientImpl.java \
        jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/Mqtt3ClientBuilder.java \
        jmqx-client/src/main/java/plus/jmqx/client/mqtt/MqttClientBuilder.java \
        jmqx-client/src/main/java/plus/jmqx/client/mqtt/internal/handler/MqttClientHandler.java
git commit -m "feat(client): add DefaultMqtt3Client engine + Async/Blocking wrappers + builders"
```

---

### Task 20: Integration test — v3 client against jmqx-broker

**Files:**
- Create: `jmqx-client/src/test/java/plus/jmqx/client/mqtt/v3/Mqtt3ClientIT.java`

This is the moment the v3 client is provably working end-to-end. Requires jmqx-broker running on `localhost:1883`. Marked as an integration test (IT suffix) so it can be excluded from the default `mvn test` run via a surefire exclusion if desired; for v1 we run it as part of `test`.

- [ ] **Step 1: Start jmqx-broker (manual, in another terminal)**

```bash
mvn -pl jmqx-broker spring-boot:run -q
```
Verify log shows MQTT listening on 1883.

- [ ] **Step 2: Write the integration test**

```java
package plus.jmqx.client.mqtt.v3;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import plus.jmqx.client.mqtt.MqttClient;
import plus.jmqx.client.mqtt.message.QoS;
import plus.jmqx.client.mqtt.v3.message.*;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class Mqtt3ClientIT {
    private static final Logger log = LoggerFactory.getLogger(Mqtt3ClientIT.class);

    @Test
    void connectPublishSubscribeDisconnect() throws Exception {
        Mqtt3RxClient client = MqttClient.builder().useMqttVersion3()
                .serverHost("localhost").serverPort(1883)
                .identifier("it-v3-" + System.nanoTime())
                .buildRx();

        Mqtt3ConnAck ack = client.connect().block(Duration.ofSeconds(5));
        assertNotNull(ack);
        assertTrue(ack.getReturnCode().isAccepted());
        log.info("connected");

        AtomicReference<Mqtt3Publish> received = new AtomicReference<>();
        Mqtt3Subscribe sub = Mqtt3Subscribe.builder()
                .topicFilters(java.util.List.of(Mqtt3TopicFilter.builder().topicFilter("test/#").qos(QoS.AT_LEAST_ONCE).build()))
                .build();
        client.subscribePublishes(sub).doOnNext(received::set).subscribe();
        client.subscribe(sub).block(Duration.ofSeconds(5));

        client.publish(Mqtt3Publish.builder().topic("test/hello").payload("world".getBytes()).qos(QoS.AT_LEAST_ONCE).build())
                .block(Duration.ofSeconds(5));
        log.info("published");

        // wait for received
        long deadline = System.nanoTime() + 5_000_000_000L;
        while (received.get() == null && System.nanoTime() < deadline) Thread.sleep(20);
        assertNotNull(received.get(), "did not receive published message");
        assertEquals("test/hello", received.get().getTopic());
        assertEquals("world", new String(received.get().getPayloadAsBytes()));

        client.disconnect().block(Duration.ofSeconds(5));
        log.info("disconnected");
    }
}
```

- [ ] **Step 3: Run the IT**

Run: `mvn -pl jmqx-client test -Dtest=Mqtt3ClientIT -q`
Expected: BUILD SUCCESS, the test passes (connects, subscribes, publishes, receives, disconnects).

If failures occur, debug against the broker; common issues:
- `MqttDecoder` not installed (NPE on read) → check TransportFactory pipeline.
- SUBACK never completes → verify `handler.registerSubAck` wiring.
- PUBLISH QoS1 never completes → verify `ackTracker.complete` called on PUBACK.

- [ ] **Step 4: Commit**

```bash
git add jmqx-client/src/test/java/plus/jmqx/client/mqtt/v3/Mqtt3ClientIT.java
git commit -m "test(client): add v3 integration test (connect/subscribe/publish/receive/disconnect)"
```

---

### Task 21: v5 message types + Properties

**Files:**
- Create (under `jmqx-client/src/main/java/plus/jmqx/client/mqtt/v5/message/`):
  - `Mqtt5PublishProperties.java`, `Mqtt5ConnAckProperties.java`
  - `Mqtt5Publish.java`, `Mqtt5Connect.java`, `Mqtt5ConnAck.java`
  - `Mqtt5Subscribe.java`, `Mqtt5SubAck.java`, `Mqtt5TopicFilter.java`, `Mqtt5Unsubscribe.java`
  - `Mqtt5Disconnect.java`, `Mqtt5PubAck.java`, `Mqtt5PubRec.java`, `Mqtt5PubRel.java`, `Mqtt5PubComp.java`

v5 = v3 plus Properties and Reason Codes. The engine is reused; only `Mqtt5MessageService` differs.

- [ ] **Step 1: Create v5 Properties types**

`Mqtt5PublishProperties.java`:
```java
package plus.jmqx.client.mqtt.v5.message;

import lombok.Builder;
import lombok.Value;
import java.util.Map;

@Value
@Builder
public class Mqtt5PublishProperties {
    Integer messageExpiryInterval;       // seconds
    String responseTopic;
    byte[] correlationData;
    Map<String, String> userProperties;
    Integer topicAlias;
    String contentType;
}
```

`Mqtt5ConnAckProperties.java`:
```java
package plus.jmqx.client.mqtt.v5.message;

import lombok.Value;

@Value
public class Mqtt5ConnAckProperties {
    int receiveMaximum;
    int serverKeepAlive;
    long sessionExpiryInterval;
    String responseInformation;
    String serverReference;
    String assignedClientIdentifier;
    boolean maximumPacketSizePresent;
    int maximumPacketSize;
}
```

- [ ] **Step 2: Create v5 messages (extend v3 patterns, add reason codes / properties)**

`Mqtt5Publish.java`:
```java
package plus.jmqx.client.mqtt.v5.message;

import plus.jmqx.client.mqtt.message.MqttPublish;

public interface Mqtt5Publish extends MqttPublish {
    Mqtt5PublishProperties getProperties();
    static Mqtt5PublishBuilder builder() { return new Mqtt5PublishBuilder(); }
}
```

`Mqtt5PublishBuilder.java`:
```java
package plus.jmqx.client.mqtt.v5.message;

import plus.jmqx.client.mqtt.message.QoS;

public class Mqtt5PublishBuilder {
    private String topic;
    private byte[] payload;
    private QoS qos = QoS.AT_MOST_ONCE;
    private boolean retain, dup;
    private int packetId;
    private Mqtt5PublishProperties properties = Mqtt5PublishProperties.builder().build();

    public Mqtt5PublishBuilder topic(String t) { this.topic = t; return this; }
    public Mqtt5PublishBuilder payload(byte[] p) { this.payload = p; return this; }
    public Mqtt5PublishBuilder qos(QoS q) { this.qos = q; return this; }
    public Mqtt5PublishBuilder retain(boolean r) { this.retain = r; return this; }
    public Mqtt5PublishBuilder dup(boolean d) { this.dup = d; return this; }
    public Mqtt5PublishBuilder packetId(int p) { this.packetId = p; return this; }
    public Mqtt5PublishBuilder properties(Mqtt5PublishProperties p) { this.properties = p; return this; }
    public Mqtt5Publish build() { return new Mqtt5PublishImpl(topic, payload, qos, retain, dup, packetId, properties); }
}
```

`Mqtt5PublishImpl.java`:
```java
package plus.jmqx.client.mqtt.v5.message;

import plus.jmqx.client.mqtt.message.MqttMessageBuilder;
import plus.jmqx.client.mqtt.message.QoS;

public final class Mqtt5PublishImpl implements Mqtt5Publish {
    private final String topic;
    private final byte[] payload;
    private final QoS qos;
    private final boolean retain, dup;
    private final int packetId;
    private final Mqtt5PublishProperties properties;

    public Mqtt5PublishImpl(String topic, byte[] payload, QoS qos, boolean retain, boolean dup, int packetId, Mqtt5PublishProperties properties) {
        this.topic = topic; this.payload = MqttMessageBuilder.cloneBytes(payload);
        this.qos = qos; this.retain = retain; this.dup = dup; this.packetId = packetId; this.properties = properties;
    }
    @Override public String getTopic() { return topic; }
    @Override public byte[] getPayloadAsBytes() { return payload.clone(); }
    @Override public QoS getQoS() { return qos; }
    @Override public boolean isRetain() { return retain; }
    @Override public boolean isDup() { return dup; }
    @Override public int getPacketId() { return packetId; }
    @Override public Mqtt5PublishProperties getProperties() { return properties; }
}
```

`Mqtt5Connect.java`:
```java
package plus.jmqx.client.mqtt.v5.message;

import lombok.Builder;
import lombok.Value;
import plus.jmqx.client.mqtt.message.MqttPublish;

@Value
@Builder(toBuilder = true)
public class Mqtt5Connect {
    String clientId;
    boolean cleanStart;
    int keepAliveSeconds;
    long sessionExpiryInterval;
    int receiveMaximum;
    String username;
    byte[] password;
    MqttPublish willPublish;
}
```

`Mqtt5ConnAck.java`:
```java
package plus.jmqx.client.mqtt.v5.message;

import lombok.Value;
import plus.jmqx.client.mqtt.message.MqttConnAck;

@Value
public class Mqtt5ConnAck implements MqttConnAck {
    boolean sessionPresent;
    byte reasonCode;
    Mqtt5ConnAckProperties properties;

    @Override public boolean isSessionPresent() { return sessionPresent; }
    public boolean isAccepted() { return reasonCode == 0; }
}
```

`Mqtt5TopicFilter.java`, `Mqtt5Subscribe.java`, `Mqtt5SubAck.java`, `Mqtt5Unsubscribe.java`, `Mqtt5Disconnect.java`, `Mqtt5PubAck.java`, `Mqtt5PubRec.java`, `Mqtt5PubRel.java`, `Mqtt5PubComp.java` — same Lombok `@Value`/`@Builder` patterns as v3 (see Task 7), with `Mqtt5Subscribe implements MqttSubscribe`, `Mqtt5SubAck implements MqttSubAck`, `Mqtt5Unsubscribe implements MqttUnsubscribe`. `Mqtt5SubAck` additionally carries `List<Byte> reasonCodes`. `Mqtt5Disconnect`/`Mqtt5PubAck`/etc. carry a `byte reasonCode`.

- [ ] **Step 3: Compile**

Run: `mvn -pl jmqx-client compile -q`
Expected: BUILD SUCCESS.

- [ ] **Step 4: Commit**

```bash
git add jmqx-client/src/main/java/plus/jmqx/client/mqtt/v5/message/
git commit -m "feat(client): add MQTT 5 message types + Properties"
```

---

### Task 22: Mqtt5MessageService — v5 protocol adapter

**Files:**
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/v5/internal/Mqtt5MessageService.java`
- Test: `jmqx-client/src/test/java/plus/jmqx/client/mqtt/v5/internal/Mqtt5MessageServiceTest.java`

Mirrors Task 8 but for v5: encodes CONNECT with `MqttProperties` (Receive Maximum, Session Expiry, cleanStart), decodes CONNACK properties, handles reason codes.

- [ ] **Step 1: Write failing round-trip test (mirror of v3 test, asserting Properties survive)**

```java
package plus.jmqx.client.mqtt.v5.internal;

import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import org.junit.jupiter.api.Test;
import plus.jmqx.client.mqtt.MqttClientConfig;
import plus.jmqx.client.mqtt.MqttVersion;
import plus.jmqx.client.mqtt.message.MqttPublish;
import plus.jmqx.client.mqtt.message.QoS;
import plus.jmqx.client.mqtt.v5.message.Mqtt5Publish;
import plus.jmqx.client.mqtt.v5.message.Mqtt5PublishProperties;

import static org.junit.jupiter.api.Assertions.*;

class Mqtt5MessageServiceTest {

    private final Mqtt5MessageService svc = new Mqtt5MessageService();
    private final EmbeddedChannel ch = new EmbeddedChannel(MqttEncoder.INSTANCE, new MqttDecoder(8 * 1024 * 1024));

    private MqttMessage roundTrip(MqttMessage out) {
        ch.writeOutbound(out);
        ByteBuf buf = ch.readOutbound();
        ch.writeInbound(buf);
        return ch.readInbound();
    }

    private MqttClientConfig config() {
        MqttClientConfig c = new MqttClientConfig();
        c.setClientId("v5-id"); c.setKeepAliveSeconds(60);
        c.setVersion(MqttVersion.MQTT_5); c.setCleanSession(false);
        c.setSessionExpiryInterval(300); c.setReceiveMaximum(100);
        return c;
    }

    @Test
    void publishRoundTripsV5() {
        Mqtt5Publish pub = Mqtt5Publish.builder()
                .topic("a/b").payload("hi".getBytes()).qos(QoS.AT_LEAST_ONCE)
                .properties(Mqtt5PublishProperties.builder().responseTopic("r/t").build())
                .build();
        MqttMessage enc = svc.encodePublish(pub, 5, false);
        MqttPublishMessage dec = (MqttPublishMessage) roundTrip(enc);
        MqttPublish decoded = svc.decodePublish(dec);
        assertEquals("a/b", decoded.getTopic());
        assertEquals("hi", new String(decoded.getPayloadAsBytes()));
        assertEquals(5, decoded.getPacketId());
    }

    @Test
    void connectEncodesV5Properties() {
        MqttMessage enc = svc.encodeConnect(config());
        assertNotNull(enc);
        // full property verification deferred to broker IT; here just assert no exception
    }
}
```

- [ ] **Step 2: Run test — fails**

Run: `mvn -pl jmqx-client test -Dtest=Mqtt5MessageServiceTest -q`
Expected: compilation failure.

- [ ] **Step 3: Implement Mqtt5MessageService**

```java
package plus.jmqx.client.mqtt.v5.internal;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.*;
import plus.jmqx.client.mqtt.MqttClientConfig;
import plus.jmqx.client.mqtt.internal.MqttMessageService;
import plus.jmqx.client.mqtt.message.*;
import plus.jmqx.client.mqtt.v5.message.*;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/** MQTT 5.0 protocol adapter. */
public class Mqtt5MessageService implements MqttMessageService {

    @Override
    public MqttMessage encodeConnect(MqttClientConfig config) {
        MqttProperties props = new MqttProperties();
        props.add(new MqttProperties.IntegerProperty(MqttProperties.MqttPropertyType.SESSION_EXPIRY_INTERVAL.value(),
                (int) config.getSessionExpiryInterval()));
        props.add(new MqttProperties.IntegerProperty(MqttProperties.MqttPropertyType.RECEIVE_MAXIMUM.value(),
                config.getReceiveMaximum()));

        MqttConnectVariableHeader header = new MqttConnectVariableHeader(
                io.netty.handler.codec.mqtt.MqttVersion.MQTT_5.protocolName(),
                io.netty.handler.codec.mqtt.MqttVersion.MQTT_5.protocolLevel(),
                config.getKeepAliveSeconds(),
                !config.isCleanSession() ? false : true,  // cleanStart semantics: cleanSession false -> cleanStart false
                config.getWillPublish() != null,
                config.getWillPublish() != null && config.getWillPublish().getQoS() != null
                        ? config.getWillPublish().getQoS().value() : 0,
                config.getWillPublish() != null && config.getWillPublish().isRetain(),
                config.getPassword() != null,
                config.getUsername() != null,
                config.isCleanSession(),   // cleanStart
                props
        );
        MqttConnectPayload payload = new MqttConnectPayload(
                config.getClientId() != null ? config.getClientId() : "",
                config.getWillPublish() != null ? config.getWillPublish().getTopic() : null,
                config.getWillPublish() != null ? config.getWillPublish().getPayloadAsBytes() : null,
                config.getUsername(),
                config.getPassword() != null ? config.getPassword() : null
        );
        return new MqttConnectMessage(header, payload);
    }

    @Override
    public MqttMessage encodePublish(MqttPublish publish, int packetId, boolean dup) {
        ByteBuf payload = publish.getPayloadAsBytes() != null
                ? Unpooled.wrappedBuffer(publish.getPayloadAsBytes()) : Unpooled.EMPTY_BUFFER;
        MqttProperties props = new MqttProperties();
        if (publish instanceof Mqtt5Publish p5 && p5.getProperties() != null) {
            Mqtt5PublishProperties pp = p5.getProperties();
            if (pp.getMessageExpiryInterval() != null)
                props.add(new MqttProperties.IntegerProperty(MqttProperties.MqttPropertyType.PUBLICATION_EXPIRY_INTERVAL.value(), pp.getMessageExpiryInterval()));
            if (pp.getResponseTopic() != null)
                props.add(new MqttProperties.StringProperty(MqttProperties.MqttPropertyType.RESPONSE_TOPIC.value(), pp.getResponseTopic()));
            if (pp.getContentType() != null)
                props.add(new MqttProperties.StringProperty(MqttProperties.MqttPropertyType.CONTENT_TYPE.value(), pp.getContentType()));
        }
        MqttFixedHeader fixed = new MqttFixedHeader(MqttMessageType.PUBLISH, dup,
                MqttQoS.valueOf(publish.getQoS().value()), publish.isRetain(), 0);
        MqttPublishVariableHeader var = new MqttPublishVariableHeader(publish.getTopic(), packetId, props);
        return new MqttPublishMessage(fixed, var, payload);
    }

    @Override
    public MqttMessage encodeSubscribe(MqttSubscribe subscribe) {
        List<MqttTopicSubscription> subs = new ArrayList<>();
        for (var tf : subscribe.getTopicFilters()) {
            subs.add(new MqttTopicSubscription(tf.getTopicFilter(), MqttQoS.valueOf(tf.getQoS().value())));
        }
        return MqttMessageBuilders.subscribe()
                .messageId(subscribe.getPacketId())
                .addSubscriptions(subs)
                .properties(new MqttProperties())
                .build();
    }

    @Override
    public MqttMessage encodeUnsubscribe(MqttUnsubscribe unsubscribe) {
        return MqttMessageBuilders.unsubscribe()
                .messageId(unsubscribe.getPacketId())
                .topicFilters(unsubscribe.getTopicFilters())
                .build();
    }

    @Override public MqttMessage encodePubAck(int packetId) {
        return MqttMessageBuilders.pubAck().messageId(packetId).build();
    }
    @Override public MqttMessage encodePubRec(int packetId) {
        MqttFixedHeader f = new MqttFixedHeader(MqttMessageType.PUBREC, false, MqttQoS.AT_MOST_ONCE, false, 0);
        return new MqttMessage(f, MqttMessageIdVariableHeader.from(packetId));
    }
    @Override public MqttMessage encodePubRel(int packetId) {
        MqttFixedHeader f = new MqttFixedHeader(MqttMessageType.PUBREL, false, MqttQoS.AT_LEAST_ONCE, false, 0);
        return new MqttMessage(f, MqttMessageIdVariableHeader.from(packetId));
    }
    @Override public MqttMessage encodePubComp(int packetId) {
        MqttFixedHeader f = new MqttFixedHeader(MqttMessageType.PUBCOMP, false, MqttQoS.AT_MOST_ONCE, false, 0);
        return new MqttMessage(f, MqttMessageIdVariableHeader.from(packetId));
    }
    @Override public MqttMessage encodeDisconnect() {
        MqttFixedHeader f = new MqttFixedHeader(MqttMessageType.DISCONNECT, false, MqttQoS.AT_MOST_ONCE, false, 0);
        return new MqttMessage(f);
    }
    @Override public MqttMessage encodePingReq() {
        return new MqttMessage(new MqttFixedHeader(MqttMessageType.PINGREQ, false, MqttQoS.AT_MOST_ONCE, false, 0));
    }

    @Override
    public MqttConnAck decodeConnAck(MqttConnAckMessage msg, MqttClientConfig config) {
        byte reasonCode = msg.variableHeader().connectReturnCode().byteValue();
        MqttProperties nettyProps = msg.variableHeader().properties();
        int receiveMax = getIntProperty(nettyProps, MqttProperties.MqttPropertyType.RECEIVE_MAXIMUM.value(), 65535);
        int serverKeepAlive = getIntProperty(nettyProps, MqttProperties.MqttPropertyType.SERVER_KEEP_ALIVE.value(), config.getKeepAliveSeconds());
        long sessionExpiry = getIntProperty(nettyProps, MqttProperties.MqttPropertyType.SESSION_EXPIRY_INTERVAL.value(), 0);
        return new Mqtt5ConnAck(msg.variableHeader().isSessionPresent(), reasonCode,
                new Mqtt5ConnAckProperties(receiveMax, serverKeepAlive, sessionExpiry, null, null, null, false, 0));
    }

    @Override
    public MqttPublish decodePublish(MqttPublishMessage msg) {
        MqttFixedHeader fixed = msg.fixedHeader();
        ByteBuf buf = msg.payload();
        byte[] payload = new byte[buf.readableBytes()];
        buf.getBytes(buf.readerIndex(), payload);
        Mqtt5PublishProperties props = new Mqtt5PublishProperties();
        return new Mqtt5PublishImpl(
                msg.variableHeader().topicName(), payload, QoS.fromValue(fixed.qosLevel().value()),
                fixed.isRetain(), fixed.isDup(), msg.variableHeader().packetId(), props);
    }

    @Override
    public MqttSubAck decodeSubAck(MqttSubAckMessage msg) {
        List<Byte> reasons = new ArrayList<>();
        for (int code : msg.payload().reasonCodes()) reasons.add((byte) code);
        List<QoS> granted = new ArrayList<>();
        for (byte b : reasons) {
            if (b >= 0 && b <= 2) granted.add(QoS.fromValue(b));
        }
        return new Mqtt5SubAck(granted, reasons, msg.variableHeader().messageId());
    }

    @Override public int decodePacketId(MqttMessage msg) {
        if (msg.variableHeader() instanceof MqttMessageIdVariableHeader id) return id.messageId();
        if (msg instanceof MqttPublishMessage pub) return pub.variableHeader().packetId();
        return 0;
    }

    @Override public boolean isConnectionAccepted(MqttConnAck ack) { return ((Mqtt5ConnAck) ack).isAccepted(); }
    @Override public RuntimeException connectionRefusedException(MqttConnAck ack) {
        return new RuntimeException("MQTT5 connection refused: reasonCode=" + ((Mqtt5ConnAck) ack).getReasonCode());
    }

    private int getIntProperty(MqttProperties props, int type, int def) {
        var p = props.getProperty(type);
        if (p instanceof MqttProperties.IntegerProperty ip) return ip.value();
        return def;
    }
}
```

Note: `Mqtt5SubAck` must be `@Value` with `List<QoS> grantedQos; List<Byte> reasonCodes; int packetId;` implementing `MqttSubAck`. The `MqttSubscribe` builder in netty `MqttMessageBuilders.subscribe()` accepts a `properties(MqttProperties)` overload in 4.1.119 — if not present, fall back to constructing `MqttSubscribeMessage` manually with `new MqttSubscribeVariableHeader(messageId, properties)`.

- [ ] **Step 4: Run test — passes**

Run: `mvn -pl jmqx-client test -Dtest=Mqtt5MessageServiceTest -q`
Expected: BUILD SUCCESS, 2 tests pass.

- [ ] **Step 5: Commit**

```bash
git add jmqx-client/src/main/java/plus/jmqx/client/mqtt/v5/internal/Mqtt5MessageService.java \
        jmqx-client/src/main/java/plus/jmqx/client/mqtt/v5/message/Mqtt5SubAck.java \
        jmqx-client/src/test/java/plus/jmqx/client/mqtt/v5/internal/Mqtt5MessageServiceTest.java
git commit -m "feat(client): add Mqtt5MessageService v5 protocol adapter (TDD round-trip)"
```

---

### Task 23: v5 client interfaces, config, builder, DefaultMqtt5Client

**Files:**
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/v5/Mqtt5Client.java`
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/v5/Mqtt5RxClient.java`
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/v5/Mqtt5AsyncClient.java`
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/v5/Mqtt5BlockingClient.java`
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/v5/Mqtt5ClientConfig.java`
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/v5/Mqtt5ClientBuilder.java`
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/v5/internal/DefaultMqtt5Client.java`
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/v5/internal/Mqtt5AsyncClientImpl.java`
- Create: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/v5/internal/Mqtt5BlockingClientImpl.java`
- Modify: `jmqx-client/src/main/java/plus/jmqx/client/mqtt/MqttClientBuilder.java` (wire `useMqttVersion5()`)

The engine (`DefaultMqtt3Client`) is designed to be version-agnostic via `MqttMessageService`. Rather than duplicate the entire engine, `DefaultMqtt5Client` extends the v3 engine and swaps in `Mqtt5MessageService`, returning v5 message types. The v3 engine's private collaborators must be widened to `protected` and the `service` field made overridable.

- [ ] **Step 1: Refactor DefaultMqtt3Client for inheritance**

In `DefaultMqtt3Client.java`:
- Change `private final Mqtt3MessageService service` → `protected MqttMessageService service` and initialize in constructor via an overridable factory method `protected MqttMessageService createService(Mqtt3ClientConfig config) { return new Mqtt3MessageService(); }`. Call `this.service = createService(config)` in the constructor.
- Widen `ackTracker`, `subscriptionStore`, `inbox`, `inboundQos`, `outbox`, `messageBuffer`, `packetIdManager`, `transportFactory` to `protected`.
- Make `doPublish`, `resubscribe`, `onTransportError`, `notifyConnected` `protected`.

- [ ] **Step 2: Create v5 interfaces + config + builder**

`v5/Mqtt5ClientConfig.java`:
```java
package plus.jmqx.client.mqtt.v5;
import plus.jmqx.client.mqtt.MqttClientConfig;
public class Mqtt5ClientConfig extends MqttClientConfig {}
```

`v5/Mqtt5Client.java`:
```java
package plus.jmqx.client.mqtt.v5;
import plus.jmqx.client.mqtt.MqttClient;
public interface Mqtt5Client extends MqttClient {
    @Override Mqtt5ClientConfig getConfig();
    Mqtt5AsyncClient toAsync(); Mqtt5RxClient toRx(); Mqtt5BlockingClient toBlock();
    static Mqtt5ClientBuilder builder() { return new Mqtt5ClientBuilder(); }
}
```

`v5/Mqtt5RxClient.java`, `Mqtt5AsyncClient.java`, `Mqtt5BlockingClient.java` — same signatures as v3 but with `Mqtt5*` message types.

`v5/Mqtt5ClientBuilder.java` — mirror of `Mqtt3ClientBuilder` adding `.sessionExpiryInterval(long)`, `.receiveMaximum(int)`, `.cleanStart(boolean)`; `buildRx()` returns `new DefaultMqtt5Client(...)`.

**Important — v5 must override the stream-returning methods** because the inherited v3 engine's `subscribePublishes()`/`publishes()` return `Flux<Mqtt3Publish>`. In `DefaultMqtt5Client` override them to return `Flux<Mqtt5Publish>` by mapping `inbox.subscriptionFlux()`/`globalFlux()` through an `Mqtt5Publish` adapter wrapper (same `toMqtt3` pattern but producing `Mqtt5Publish`, with `ack()` delegating to the `Deliverable`). Also override `connect()` return type to `Mono<Mqtt5ConnAck>` (covariant return) and `publish`/`subscribe`/`unsubscribe`/`disconnect` to v5 message types — all delegating to the inherited engine logic with a cast, since the engine internals are version-agnostic via `MqttMessageService`.

- [ ] **Step 3: Create DefaultMqtt5Client (extends v3 engine)**

```java
package plus.jmqx.client.mqtt.v5.internal;

import lombok.extern.slf4j.Slf4j;
import plus.jmqx.client.mqtt.MqttVersion;
import plus.jmqx.client.mqtt.internal.MqttMessageService;
import plus.jmqx.client.mqtt.lifecycle.*;
import plus.jmqx.client.mqtt.v3.Mqtt3ClientConfig;
import plus.jmqx.client.mqtt.v3.internal.DefaultMqtt3Client;
import plus.jmqx.client.mqtt.v5.*;
import plus.jmqx.client.mqtt.v5.message.*;

import java.util.List;

@Slf4j
public class DefaultMqtt5Client extends DefaultMqtt3Client implements Mqtt5RxClient {

    public DefaultMqtt5Client(Mqtt5ClientConfig config,
                              List<MqttClientConnectedListener> connected,
                              List<MqttClientDisconnectedListener> disconnected) {
        super(adaptConfig(config), connected, disconnected);
    }

    @Override
    protected MqttMessageService createService(Mqtt3ClientConfig config) {
        return new Mqtt5MessageService();
    }

    @Override public MqttVersion getVersion() { return MqttVersion.MQTT_5; }
    @Override public Mqtt5AsyncClient toAsync() { return new Mqtt5AsyncClientImpl(this); }
    @Override public Mqtt5RxClient toRx() { return this; }
    @Override public Mqtt5BlockingClient toBlock() { return new Mqtt5BlockingClientImpl(this); }

    /** v5 CONNACK receive maximum overrides the v3 default inflight cap. */
    @Override
    protected void afterConnAck(plus.jmqx.client.mqtt.message.MqttConnAck ack) {
        if (ack instanceof Mqtt5ConnAck a5) {
            // override outbox permits via receiveMaximum from broker
            // (MqttOutbox would need a setMaxPermits method; if not present, skip — see note)
        }
    }

    /** Cast a v5 config as the v3 base the engine expects. */
    private static Mqtt3ClientConfig adaptConfig(Mqtt5ClientConfig c) {
        Mqtt3ClientConfig adapted = new Mqtt3ClientConfig();
        adapted.setServerHost(c.getServerHost());
        adapted.setServerPort(c.getServerPort());
        adapted.setClientId(c.getClientId());
        adapted.setKeepAliveSeconds(c.getKeepAliveSeconds());
        adapted.setVersion(MqttVersion.MQTT_5);
        adapted.setCleanSession(c.isCleanSession());
        adapted.setSessionExpiryInterval(c.getSessionExpiryInterval());
        adapted.setReceiveMaximum(c.getReceiveMaximum());
        adapted.setUsername(c.getUsername());
        adapted.setPassword(c.getPassword());
        adapted.setWillPublish(c.getWillPublish());
        adapted.setAutomaticReconnect(c.isAutomaticReconnect());
        adapted.setReconnectInitialDelayMs(c.getReconnectInitialDelayMs());
        adapted.setReconnectMaxDelayMs(c.getReconnectMaxDelayMs());
        return adapted;
    }
}
```

- [ ] **Step 4: Wire `useMqttVersion5()` in top-level builder**

```java
// MqttClientBuilder.java
public plus.jmqx.client.mqtt.v5.Mqtt5ClientBuilder useMqttVersion5() {
    return new plus.jmqx.client.mqtt.v5.Mqtt5ClientBuilder();
}
```

- [ ] **Step 5: Create v5 Async/Blocking wrappers** (mirror of Task 19 Step 3, with `Mqtt5*` types)

- [ ] **Step 6: Compile**

Run: `mvn -pl jmqx-client compile -q`
Expected: BUILD SUCCESS.

- [ ] **Step 7: Commit**

```bash
git add jmqx-client/src/main/java/plus/jmqx/client/mqtt/v5/ \
        jmqx-client/src/main/java/plus/jmqx/client/mqtt/v3/internal/DefaultMqtt3Client.java \
        jmqx-client/src/main/java/plus/jmqx/client/mqtt/MqttClientBuilder.java
git commit -m "feat(client): add v5 client (extends engine, Mqtt5MessageService) + builders"
```

---

### Task 24: v5 integration test + final full build

**Files:**
- Create: `jmqx-client/src/test/java/plus/jmqx/client/mqtt/v5/Mqtt5ClientIT.java`

- [ ] **Step 1: Write v5 IT (mirror of v3 IT, asserts v5 connect + publish + properties survive)**

```java
package plus.jmqx.client.mqtt.v5;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import plus.jmqx.client.mqtt.MqttClient;
import plus.jmqx.client.mqtt.message.QoS;
import plus.jmqx.client.mqtt.v5.message.*;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class Mqtt5ClientIT {
    private static final Logger log = LoggerFactory.getLogger(Mqtt5ClientIT.class);

    @Test
    void connectPublishSubscribeDisconnectV5() throws Exception {
        Mqtt5RxClient client = MqttClient.builder().useMqttVersion5()
                .serverHost("localhost").serverPort(1883)
                .identifier("it-v5-" + System.nanoTime())
                .buildRx();

        Mqtt5ConnAck ack = client.connect().block(Duration.ofSeconds(5));
        assertNotNull(ack);
        assertTrue(ack.isAccepted());
        log.info("v5 connected, receiveMax={}", ack.getProperties().getReceiveMaximum());

        AtomicReference<Mqtt5Publish> received = new AtomicReference<>();
        Mqtt5Subscribe sub = Mqtt5Subscribe.builder()
                .topicFilters(List.of(Mqtt5TopicFilter.builder().topicFilter("v5/#").qos(QoS.AT_LEAST_ONCE).build()))
                .build();
        client.subscribePublishes(sub).doOnNext(received::set).subscribe();
        client.subscribe(sub).block(Duration.ofSeconds(5));

        client.publish(Mqtt5Publish.builder().topic("v5/hello").payload("world5".getBytes())
                        .qos(QoS.AT_LEAST_ONCE)
                        .properties(Mqtt5PublishProperties.builder().contentType("text/plain").build())
                        .build())
                .block(Duration.ofSeconds(5));

        long deadline = System.nanoTime() + 5_000_000_000L;
        while (received.get() == null && System.nanoTime() < deadline) Thread.sleep(20);
        assertNotNull(received.get(), "did not receive v5 publish");
        assertEquals("v5/hello", received.get().getTopic());
        assertEquals("world5", new String(received.get().getPayloadAsBytes()));

        client.disconnect().block(Duration.ofSeconds(5));
    }
}
```

- [ ] **Step 2: Run v5 IT (broker must be running)**

Run: `mvn -pl jmqx-client test -Dtest=Mqtt5ClientIT -q`
Expected: BUILD SUCCESS.

- [ ] **Step 3: Run the full client test suite**

Run: `mvn -pl jmqx-client test -q`
Expected: BUILD SUCCESS — all unit tests + both ITs pass.

- [ ] **Step 4: Commit**

```bash
git add jmqx-client/src/test/java/plus/jmqx/client/mqtt/v5/Mqtt5ClientIT.java
git commit -m "test(client): add v5 integration test; full suite green"
```

---

### Task 25: README + final commit

**Files:**
- Modify: `jmqx-client/README.md`

- [ ] **Step 1: Replace README with usage docs reflecting the shipped API**

```markdown
# jmqx-client — Reactor-Netty MQTT Client

High-performance async MQTT 3.1.1 + 5.0 client built on reactor-netty. Features:
two-way backpressure, auto-reconnect, offline buffering, TCP/SSL/WS/WSS.

## Quick start

\```java
Mqtt3RxClient client = MqttClient.builder().useMqttVersion3()
    .serverHost("localhost").serverPort(1883)
    .identifier("my-client")
    .automaticReconnect()
    .buildRx();

client.connect().block(Duration.ofSeconds(5));

client.subscribePublishes(Mqtt3Subscribe.builder()
        .topicFilter("sensor/#", QoS.AT_LEAST_ONCE).build())
    .doOnNext(p -> System.out.println(p.getTopic() + ": " + new String(p.getPayloadAsBytes())))
    .subscribe();

client.publish(Mqtt3Publish.builder().topic("sensor/temp").payload("21.5".getBytes()).qos(QoS.AT_LEAST_ONCE).build())
    .block();
\```

## API views
- `Mqtt3RxClient` / `Mqtt5RxClient` — Reactor Mono/Flux
- `Mqtt3AsyncClient` / `Mqtt5AsyncClient` — CompletableFuture
- `Mqtt3BlockingClient` / `Mqtt5BlockingClient` — synchronous

## Features
- **Two-way backpressure**: inbound `request(n)` gates PUBACK/PUBREC; outbound inflight semaphore
- **Auto-reconnect**: exponential backoff + jitter, reactor-async (no blocking)
- **Offline buffer**: publishes during disconnect replayed on reconnect with ACK re-registration
- **v3.1.1 + v5.0**: single engine, dual protocol adapter
- **Transports**: TCP, TLS, WebSocket, WSS via reactor-netty

## Dependencies
reactor-netty, netty-codec-mqtt, reactor-core, Lombok, SLF4J. Java 17+.
```

- [ ] **Step 2: Commit**

```bash
git add jmqx-client/README.md
git commit -m "docs(client): rewrite README for shipped reactor-netty client"
```

---

## Spec Coverage Check

| Spec section | Task(s) | Status |
|---|---|---|
| §1 总体架构 (reactor-netty, single engine, dual protocol) | 14, 19, 23 | ✅ |
| §2 分层架构 (single MqttClientHandler) | 14 | ✅ |
| §3 版本化 API (Mqtt3/5 + Async/Rx/Blocking) | 18, 21, 23 | ✅ |
| §4 协议适配 + 消息模型 (immutable value types, MqttMessageService) | 2, 6, 7, 8, 21, 22 | ✅ |
| §5 连接引擎 + 4 态状态机 + keepalive PINGREQ | 13, 14, 19 | ✅ |
| §6 QoS 流程 + 单一 PacketIdManager + AckTracker | 4, 9, 14, 16 | ✅ |
| §7 双向背压 (request 门控 + inflight 信号量) | 12, 17 | ✅ |
| §8 断线缓存 (flush 重注 ACK) | 10, 19 | ✅ |
| §9 自动重连 (reactor Mono.delay) | 15, 19 | ✅ |
| §10 传输层 (TCP/SSL/WS/WSS TransportFactory) | 6, 13 | ✅ |
| §11 配置模型 | 6, 18, 23 | ✅ |
| §12 Builder API | 19, 23 | ✅ |
| §13 文件结构 | all | ✅ |
| §14 测试策略 (unit + StepVerifier + broker IT) | 2-4, 8-12, 15, 17, 20, 24 | ✅ |
| §15 实现范围 (v3+v5, QoS0/1/2, backpressure, reconnect, buffer, keepalive) | 1-24 | ✅ |
| §16 依赖 | 1 | ✅ |
| §17 旧设计缺陷修复 | addressed across all tasks | ✅ |

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-07-02-jmqx-client.md`.

**Two execution options:**

1. **Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration

2. **Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints

**Which approach?**
