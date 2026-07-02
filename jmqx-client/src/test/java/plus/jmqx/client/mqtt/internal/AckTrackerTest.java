package plus.jmqx.client.mqtt.internal;

import org.junit.jupiter.api.Test;
import plus.jmqx.client.mqtt.message.MqttPublish;
import plus.jmqx.client.mqtt.message.MqttPublishResult;
import plus.jmqx.client.mqtt.message.QoS;
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
        assertNull(tracker.remove(10));  // 已移除
    }

    @Test
    void pubrecKeepsPendingUntilComplete() {
        AckTracker tracker = new AckTracker();
        Sinks.One<MqttPublishResult> sink = Sinks.one();
        tracker.register(20, new PendingOutbound(stub(), sink));

        tracker.markReceived(20);          // PUBREC 到达
        assertFalse(tracker.isComplete(20));  // 仍等待 PUBCOMP
        assertEquals(1, tracker.size());

        StepVerifier.create(sink.asMono())
                .then(() -> tracker.complete(20, new MqttPublishResultImpl(stub(), null)))
                .expectNextMatches(r -> r.getError() == null)
                .verifyComplete();
        assertTrue(tracker.isComplete(20));
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

    @Test
    void failAllEmptiesMap() {
        AckTracker tracker = new AckTracker();
        tracker.register(1, new PendingOutbound(stub(), Sinks.one()));
        tracker.register(2, new PendingOutbound(stub(), Sinks.one()));
        assertEquals(2, tracker.size());
        tracker.failAll(new RuntimeException("session expired"));
        assertEquals(0, tracker.size());
    }

    private MqttPublish stub() {
        return new MqttPublish() {
            @Override public String getTopic() { return "t"; }
            @Override public byte[] getPayloadAsBytes() { return new byte[0]; }
            @Override public QoS getQoS() { return QoS.AT_LEAST_ONCE; }
            @Override public boolean isRetain() { return false; }
            @Override public boolean isDup() { return false; }
            @Override public int getPacketId() { return 0; }
        };
    }
}
