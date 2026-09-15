package plus.jmqx.client.mqtt.internal.handler;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.junit.jupiter.api.Test;
import plus.jmqx.client.mqtt.MqttClientConfig;
import plus.jmqx.client.mqtt.internal.AckTracker;
import plus.jmqx.client.mqtt.internal.InboundQos;
import plus.jmqx.client.mqtt.internal.MqttInbox;
import plus.jmqx.client.mqtt.v3.internal.Mqtt3MessageService;
import reactor.core.publisher.Sinks;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 服务端 DISCONNECT 必须回调引擎，不能只打 debug。
 */
class MqttClientHandlerDisconnectTest {

    @Test
    void inboundDisconnectNotifiesPeerListener() {
        AtomicInteger calls = new AtomicInteger();
        MqttClientHandler handler = new MqttClientHandler(
                new MqttClientConfig(),
                new Mqtt3MessageService(),
                new AckTracker(),
                new MqttInbox(8),
                new InboundQos(),
                Sinks.one(),
                cause -> calls.incrementAndGet());
        EmbeddedChannel channel = new EmbeddedChannel(handler);

        handler.handleInbound(channel, disconnect());

        assertEquals(1, calls.get());
        channel.finishAndReleaseAll();
    }

    private static MqttMessage disconnect() {
        return new MqttMessage(new MqttFixedHeader(
                MqttMessageType.DISCONNECT, false, MqttQoS.AT_MOST_ONCE, false, 0));
    }
}
