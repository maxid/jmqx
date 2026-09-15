package plus.jmqx.client.mqtt.internal;

import org.junit.jupiter.api.Test;
import plus.jmqx.client.mqtt.MqttClientConfig;
import plus.jmqx.client.mqtt.MqttClientState;
import plus.jmqx.client.mqtt.lifecycle.MqttClientConnectedListener;
import plus.jmqx.client.mqtt.lifecycle.MqttClientDisconnectedContext;
import plus.jmqx.client.mqtt.lifecycle.MqttClientDisconnectedListener;
import plus.jmqx.client.mqtt.message.MqttSubscribe;
import plus.jmqx.client.mqtt.message.MqttTopicFilter;
import plus.jmqx.client.mqtt.message.MqttUnsubscribe;
import plus.jmqx.client.mqtt.v3.internal.Mqtt3MessageService;

import java.net.SocketException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * 对端掐 TCP / 入站结束必须变成 SERVER 断开，不能静默留在 CONNECTED。
 */
class MqttClientEngineDisconnectTest {

    @Test
    void connectionResetNotifiesServerDisconnect() {
        AtomicReference<MqttClientDisconnectedContext> seen = new AtomicReference<>();
        TestEngine engine = connectedEngine(seen::set);

        engine.onInboundStreamError(new SocketException("Connection reset"));

        assertEquals(MqttClientState.DISCONNECTED, engine.getState());
        assertNotNull(seen.get());
        assertEquals(MqttClientDisconnectedContext.DisconnectSource.SERVER, seen.get().getSource());
    }

    @Test
    void inboundCompleteNotifiesWhenConnected() {
        AtomicReference<MqttClientDisconnectedContext> seen = new AtomicReference<>();
        TestEngine engine = connectedEngine(seen::set);

        engine.onInboundCompleted();

        assertEquals(MqttClientState.DISCONNECTED, engine.getState());
        assertEquals(MqttClientDisconnectedContext.DisconnectSource.SERVER, seen.get().getSource());
    }

    @Test
    void secondPeerCloseDoesNotNotifyTwice() {
        AtomicInteger calls = new AtomicInteger();
        TestEngine engine = connectedEngine(ctx -> calls.incrementAndGet());

        engine.onInboundStreamError(new SocketException("Connection reset"));
        engine.onInboundCompleted();
        engine.onTransportError(new RuntimeException("connection disposed"));

        assertEquals(1, calls.get());
        assertEquals(MqttClientState.DISCONNECTED, engine.getState());
    }

    @Test
    void inboundErrorWhileDisconnectingDoesNotNotify() {
        AtomicReference<MqttClientDisconnectedContext> seen = new AtomicReference<>();
        TestEngine engine = connectedEngine(seen::set);
        engine.markDisconnecting();

        engine.onInboundStreamError(new SocketException("Connection reset"));

        assertNull(seen.get());
    }

    private static TestEngine connectedEngine(MqttClientDisconnectedListener listener) {
        TestEngine engine = new TestEngine(new MqttClientConfig(), List.of(), List.of(listener));
        engine.markConnected();
        return engine;
    }

    private static final class TestEngine extends MqttClientEngine {

        private TestEngine(MqttClientConfig config,
                           List<MqttClientConnectedListener> connectedListeners,
                           List<MqttClientDisconnectedListener> disconnectedListeners) {
            super(config, connectedListeners, disconnectedListeners);
        }

        void markConnected() {
            state.set(MqttClientState.CONNECTED);
        }

        void markDisconnecting() {
            state.set(MqttClientState.DISCONNECTING);
        }

        @Override
        protected MqttMessageService createService(MqttClientConfig cfg) {
            return new Mqtt3MessageService();
        }

        @Override
        protected MqttSubscribe buildResubscribe(List<MqttTopicFilter> filters, int packetId) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected MqttSubscribe copySubscribeWithPacketId(MqttSubscribe subscribe, int packetId) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected MqttUnsubscribe copyUnsubscribeWithPacketId(MqttUnsubscribe unsubscribe, int packetId) {
            throw new UnsupportedOperationException();
        }
    }
}
