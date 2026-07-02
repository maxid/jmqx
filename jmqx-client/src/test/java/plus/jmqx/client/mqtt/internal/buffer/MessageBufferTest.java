package plus.jmqx.client.mqtt.internal.buffer;

import org.junit.jupiter.api.Test;
import plus.jmqx.client.mqtt.internal.MqttPublishResultImpl;
import plus.jmqx.client.mqtt.message.MqttPublish;
import plus.jmqx.client.mqtt.message.QoS;
import plus.jmqx.client.mqtt.v3.message.Mqtt3Publish;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MessageBufferTest {

    @Test
    void offerUntilMaxSizeReturnsError() {
        MessageBuffer buf = new MessageBuffer(2, Long.MAX_VALUE);
        // 前 2 个 offer 入队（返回的 Mono 在 flush 前不完成）
        buf.offer(pub("a")).subscribe();
        buf.offer(pub("b")).subscribe();
        // 第 3 个超出容量 -> offer Mono 立即 error
        StepVerifier.create(buf.offer(pub("c")))
                .expectError(MessageBufferFullException.class)
                .verify();
        assertEquals(2, buf.size());
    }

    @Test
    void offerUntilMaxBytesReturnsError() {
        MessageBuffer buf = new MessageBuffer(Integer.MAX_VALUE, 10L);
        buf.offer(pub("123456")).subscribe(); // 6 bytes < 10
        StepVerifier.create(buf.offer(pub("123456")))
                .expectError(MessageBufferFullException.class)
                .verify();
    }

    @Test
    void flushInvokesWriterInOrderAndCompletesResults() {
        MessageBuffer buf = new MessageBuffer(10, Long.MAX_VALUE);
        var s1 = buf.offer(pub("a"));
        var s2 = buf.offer(pub("b"));
        List<String> order = new ArrayList<>();

        var flushMono = buf.flush(p -> {
            order.add(new String(p.getPayloadAsBytes()));
            return Mono.just(new MqttPublishResultImpl(p, null));
        });

        StepVerifier.create(flushMono).verifyComplete();
        assertEquals(List.of("a", "b"), order);
        assertEquals(0, buf.size());

        // 两个原始 offer Mono 现已成功完成
        StepVerifier.create(s1).expectNextCount(1).verifyComplete();
        StepVerifier.create(s2).expectNextCount(1).verifyComplete();
    }

    @Test
    void flushPropagatesWriterError() {
        MessageBuffer buf = new MessageBuffer(10, Long.MAX_VALUE);
        var s1 = buf.offer(pub("a"));
        buf.flush(p -> Mono.error(new RuntimeException("send failed"))).onErrorResume(e -> Mono.empty()).block();
        // writer 错误 -> 原 offer Mono 也以 error 完成
        StepVerifier.create(s1).expectError(RuntimeException.class).verify();
        assertEquals(0, buf.size());
    }

    @Test
    void clearEmptiesAndFailsPending() {
        MessageBuffer buf = new MessageBuffer(10, Long.MAX_VALUE);
        var s1 = buf.offer(pub("a"));
        buf.clear();
        assertEquals(0, buf.size());
        // clear 后，pending offer 不会完成（已从队列移除且 sink 未触发）
        StepVerifier.create(s1).expectTimeout(java.time.Duration.ofMillis(100)).verify();
    }

    private MqttPublish pub(String payload) {
        return Mqtt3Publish.builder().topic("t").payload(payload.getBytes()).qos(QoS.AT_LEAST_ONCE).build();
    }
}
