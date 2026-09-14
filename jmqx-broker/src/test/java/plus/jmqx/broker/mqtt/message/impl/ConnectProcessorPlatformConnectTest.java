package plus.jmqx.broker.mqtt.message.impl;

import io.netty.handler.codec.mqtt.MqttConnectVariableHeader;
import io.netty.handler.codec.mqtt.MqttVersion;
import org.junit.jupiter.api.Test;
import plus.jmqx.broker.mqtt.channel.MqttSession;
import plus.jmqx.broker.mqtt.message.dispatch.ConnectMessage;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * CONNECT 平台回调必须带上 Keep Alive 秒数。
 */
class ConnectProcessorPlatformConnectTest {

    @Test
    void copiesKeepAliveSecondsFromConnectHeader() {
        MqttSession session = new MqttSession();
        session.setClientId("dev-1");
        session.setUsername("user-1");
        MqttConnectVariableHeader header = new MqttConnectVariableHeader(
                MqttVersion.MQTT_3_1_1.protocolName(),
                MqttVersion.MQTT_3_1_1.protocolLevel(),
                true,
                false,
                false,
                0,
                false,
                true,
                90);
        ConnectMessage message = ConnectProcessor.toPlatformConnect(session, header);
        assertEquals("dev-1", message.getClientId());
        assertEquals("user-1", message.getUsername());
        assertEquals("MQTT", message.getProtocolName());
        assertEquals(4, message.getVersion());
        assertEquals(90, message.getKeepAlive());
    }
}
