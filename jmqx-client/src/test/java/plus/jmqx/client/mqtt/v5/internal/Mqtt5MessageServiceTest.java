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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class Mqtt5MessageServiceTest {

    private final Mqtt5MessageService svc = new Mqtt5MessageService();
    private final EmbeddedChannel     ch  = new EmbeddedChannel(MqttEncoder.INSTANCE, new MqttDecoder(8 * 1024 * 1024));

    private MqttMessage roundTrip(MqttMessage out) {
        ch.writeOutbound(out);
        ByteBuf buf = ch.readOutbound();
        ch.writeInbound(buf);
        return ch.readInbound();
    }

    private MqttClientConfig config() {
        MqttClientConfig c = new MqttClientConfig();
        c.setClientId("v5-id");
        c.setKeepAliveSeconds(60);
        c.setVersion(MqttVersion.MQTT_5);
        c.setCleanSession(false);
        c.setSessionExpiryInterval(300);
        c.setReceiveMaximum(100);
        return c;
    }

    @Test
    void publishRoundTripsV5() {
        Mqtt5Publish pub = Mqtt5Publish.builder()
                .topic("a/b")
                .payload("hi".getBytes())
                .qos(QoS.AT_LEAST_ONCE)
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
    }
}
