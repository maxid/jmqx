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
        AtomicInteger connectCalls = new AtomicInteger();
        MqttAutoReconnect r = new MqttAutoReconnect(100, 10_000,
                () -> {
                    connectCalls.incrementAndGet();
                    return Mono.empty();   // 连接立即"完成"（空），会触发再次 onDisconnected? 此处直接返回 empty 不再断开
                }, vts);

        // 触发首次重连
        r.onDisconnected(disconnectCtx(new MqttClientReconnector(0, true)));

        StepVerifier.withVirtualTime(() -> Mono.never(), () -> vts, Long.MAX_VALUE)
                .thenAwait(Duration.ofMillis(150))   // 首次延迟 ~100ms
                .then(() -> assertEquals(1, connectCalls.get(), "应在 ~100ms 后首次重连"))
                .thenAwait(Duration.ofMillis(300))   // 第二次 ~200ms
                .then(() -> assertTrue(connectCalls.get() >= 1))
                .thenCancel()
                .verify();
    }

    @Test
    void userDisconnectDoesNotReconnect() {
        AtomicInteger calls = new AtomicInteger();
        MqttAutoReconnect r = new MqttAutoReconnect(100, 10_000,
                () -> {
                    calls.incrementAndGet();
                    return Mono.empty();
                }, VirtualTimeScheduler.create());
        MqttClientDisconnectedContext ctx = new MqttClientDisconnectedContext(
                new MqttClientConfig(),
                MqttClientDisconnectedContext.DisconnectSource.USER,
                null, new MqttClientReconnector(0, true));
        r.onDisconnected(ctx);
        assertEquals(0, calls.get(), "用户主动断开不应触发重连");
    }

    private MqttClientDisconnectedContext disconnectCtx(MqttClientReconnector rc) {
        return new MqttClientDisconnectedContext(new MqttClientConfig(),
                MqttClientDisconnectedContext.DisconnectSource.SERVER, null, rc);
    }
}
