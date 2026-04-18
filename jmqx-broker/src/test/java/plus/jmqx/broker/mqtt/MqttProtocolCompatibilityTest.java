package plus.jmqx.broker.mqtt;

import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttVersion;
import org.junit.jupiter.api.Test;
import plus.jmqx.broker.mqtt.message.MqttMessageBuilder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * MQTT 3.x/5 协议兼容性单元测试
 */
class MqttProtocolCompatibilityTest {

    /**
     * 校验 MQTT 3.1 ConnAck 返回码与属性
     */
    @Test
    void connectAckMqtt31KeepsReturnCodeAndNoProperties() {
        MqttConnAckMessage ack = MqttMessageBuilder.connectAckMessage(
                MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED,
                MqttVersion.MQTT_3_1.protocolLevel()
        );
        assertEquals(MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED, ack.variableHeader().connectReturnCode());
        assertSame(MqttProperties.NO_PROPERTIES, ack.variableHeader().properties());
    }

    /**
     * 校验 MQTT 3.1.1 ConnAck 返回码与属性
     */
    @Test
    void connectAckMqtt311KeepsReturnCodeAndNoProperties() {
        MqttConnAckMessage ack = MqttMessageBuilder.connectAckMessage(
                MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD,
                MqttVersion.MQTT_3_1_1.protocolLevel()
        );
        assertEquals(MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD, ack.variableHeader().connectReturnCode());
        assertSame(MqttProperties.NO_PROPERTIES, ack.variableHeader().properties());
    }

    /**
     * 校验 MQTT 5 ConnAck 返回码映射与属性填充
     */
    @Test
    @SuppressWarnings("rawtypes")
    void connectAckMqtt5MapsReturnCodesAndSetsProperties() {
        MqttConnAckMessage ack = MqttMessageBuilder.connectAckMessage(
                MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED,
                MqttVersion.MQTT_5.protocolLevel()
        );
        assertEquals(MqttConnectReturnCode.CONNECTION_REFUSED_CLIENT_IDENTIFIER_NOT_VALID, ack.variableHeader().connectReturnCode());

        MqttProperties props = ack.variableHeader().properties();
        assertNotNull(props);
        MqttProperties.MqttProperty retain = props.getProperty(MqttProperties.MqttPropertyType.RETAIN_AVAILABLE.value());
        MqttProperties.MqttProperty shared = props.getProperty(MqttProperties.MqttPropertyType.SHARED_SUBSCRIPTION_AVAILABLE.value());
        assertNotNull(retain);
        assertNotNull(shared);
        assertEquals(1, ((MqttProperties.IntegerProperty) retain).value());
        assertEquals(0, ((MqttProperties.IntegerProperty) shared).value());
    }

}
