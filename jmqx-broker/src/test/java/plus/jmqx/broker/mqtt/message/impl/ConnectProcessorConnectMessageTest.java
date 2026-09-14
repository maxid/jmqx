package plus.jmqx.broker.mqtt.message.impl;

import io.netty.handler.codec.mqtt.MqttConnectVariableHeader;
import io.netty.handler.codec.mqtt.MqttVersion;
import org.junit.jupiter.api.Test;
import plus.jmqx.broker.mqtt.channel.MqttSession;
import plus.jmqx.broker.mqtt.message.dispatch.ConnectMessage;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * {@link ConnectProcessor#toConnectMessage} 须从 CONNECT 可变头拷贝 Keep Alive（秒）。
 */
class ConnectProcessorConnectMessageTest {

    /**
     * Keep Alive、clientId、username、协议名与级别均来自会话与 CONNECT 头。
     */
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
        ConnectMessage message = ConnectProcessor.toConnectMessage(session, header);
        assertEquals("dev-1", message.getClientId());
        assertEquals("user-1", message.getUsername());
        assertEquals("MQTT", message.getProtocolName());
        assertEquals(4, message.getVersion());
        assertEquals(90, message.getKeepAlive());
    }
}
