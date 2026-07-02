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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class Mqtt3MessageServiceTest {

    private final Mqtt3MessageService svc = new Mqtt3MessageService();
    private final EmbeddedChannel     ch  = new EmbeddedChannel(MqttEncoder.INSTANCE, new MqttDecoder(8 * 1024 * 1024));

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
    void subscribeRoundTrips() {
        Mqtt3Subscribe sub = Mqtt3Subscribe.builder()
                .topicFilters(List.of(Mqtt3TopicFilter.builder().topicFilter("t/#").qos(QoS.AT_MOST_ONCE).build()))
                .packetId(7).build();
        MqttMessage enc = svc.encodeSubscribe(sub);
        assertNotNull(roundTrip(enc));   // 编码器接受；完整 SUBACK 解码由 broker IT 覆盖
    }

    @Test
    void connectAccepted() {
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
