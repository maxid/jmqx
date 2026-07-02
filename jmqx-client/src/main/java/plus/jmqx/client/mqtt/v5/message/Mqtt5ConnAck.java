package plus.jmqx.client.mqtt.v5.message;

import lombok.Value;
import plus.jmqx.client.mqtt.message.MqttConnAck;

/**
 * MQTT 5.0 CONNACK。
 *
 * @author maxid
 * @since 1.4.14
 */
@Value
public class Mqtt5ConnAck implements MqttConnAck {

    /**
     * 会话是否已存在
     */
    boolean                sessionPresent;
    /**
     * 连接响应码
     */
    byte                   reasonCode;
    /**
     * CONNACK 属性
     */
    Mqtt5ConnAckProperties properties;

    @Override
    public boolean isSessionPresent() {
        return sessionPresent;
    }

    /**
     * @return 连接是否被 broker 接受（reason code == 0）
     */
    public boolean isAccepted() {
        return reasonCode == 0;
    }

}
