package plus.jmqx.client.mqtt.internal;

import org.junit.jupiter.api.Test;
import plus.jmqx.client.mqtt.message.MqttPublish;
import plus.jmqx.client.mqtt.message.QoS;
import plus.jmqx.client.mqtt.v3.message.Mqtt3Publish;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

class MqttInboxBackpressureTest {

    @Test
    void ackOnlyFiredWhenDownstreamConsumes() {
        MqttInbox inbox = new MqttInbox(1024);
        AtomicBoolean ackFired = new AtomicBoolean();

        // 用初始 request(0) 验证：消费前 ack 不触发，request(1) 后触发
        StepVerifier.create(inbox.globalFlux().doOnNext(MqttPublish::ack), 0)
                .then(() -> {
                    MqttPublish pub = Mqtt3Publish.builder().topic("t").payload(new byte[0]).qos(QoS.AT_LEAST_ONCE).build();
                    inbox.deliver(pub, () -> ackFired.set(true));
                })
                .then(() -> assertFalse(ackFired.get(), "ack 在 request 前不应触发"))
                .thenRequest(1)
                .assertNext(p -> assertTrue(ackFired.get(), "ack 在消费后应触发"))
                .thenCancel()
                .verify(Duration.ofSeconds(2));
    }

    @Test
    void bufferFullReturnsFalse() {
        MqttInbox inbox = new MqttInbox(2);  // 容量 2
        AtomicBoolean ackFired = new AtomicBoolean();
        // 无订阅者 —— 缓冲填满
        for (int i = 0; i < 2; i++) {
            MqttPublish pub = Mqtt3Publish.builder().topic("t").payload(new byte[0]).qos(QoS.AT_LEAST_ONCE).build();
            assertTrue(inbox.deliver(pub, () -> ackFired.set(true)));   // 缓冲
        }
        MqttPublish pub = Mqtt3Publish.builder().topic("t").payload(new byte[0]).qos(QoS.AT_LEAST_ONCE).build();
        assertFalse(inbox.deliver(pub, () -> ackFired.set(true)), "缓冲满应返回 false（丢弃）");
        assertFalse(ackFired.get(), "缓冲中 ack 不应触发");
    }
}
