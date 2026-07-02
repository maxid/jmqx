package plus.jmqx.client.mqtt.v5.message;

import lombok.Builder;
import lombok.Value;
import plus.jmqx.client.mqtt.message.MqttPublish;

/**
 * MQTT 5.0 CONNECT。
 *
 * @author maxid
 */
@Value
@Builder(toBuilder = true)
public class Mqtt5Connect {

    String clientId;
    boolean cleanStart;
    int keepAliveSeconds;
    long sessionExpiryInterval;
    int receiveMaximum;
    String username;
    byte[] password;
    MqttPublish willPublish;
}
