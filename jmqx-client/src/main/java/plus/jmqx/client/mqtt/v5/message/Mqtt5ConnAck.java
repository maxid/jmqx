package plus.jmqx.client.mqtt.v5.message;

import lombok.Value;
import plus.jmqx.client.mqtt.message.MqttConnAck;

/**
 * MQTT 5.0 CONNACK。
 *
 * @author maxid
 */
@Value
public class Mqtt5ConnAck implements MqttConnAck {

    boolean sessionPresent;
    byte reasonCode;
    Mqtt5ConnAckProperties properties;

    @Override
    public boolean isSessionPresent() {
        return sessionPresent;
    }

    public boolean isAccepted() {
        return reasonCode == 0;
    }
}
