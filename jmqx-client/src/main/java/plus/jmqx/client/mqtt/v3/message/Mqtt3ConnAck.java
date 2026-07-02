package plus.jmqx.client.mqtt.v3.message;

import lombok.Value;
import plus.jmqx.client.mqtt.message.MqttConnAck;

/**
 * MQTT 3.1.1 CONNACK。
 *
 * @author maxid
 */
@Value
public class Mqtt3ConnAck implements MqttConnAck {

    boolean sessionPresent;
    Mqtt3ConnAckReturnCode returnCode;

    @Override
    public boolean isSessionPresent() {
        return sessionPresent;
    }
}
