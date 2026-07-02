package plus.jmqx.client.mqtt.v3.message;

import lombok.Builder;
import lombok.Value;
import plus.jmqx.client.mqtt.message.MqttPublish;

/**
 * MQTT 3.1.1 CONNECT 参数。
 *
 * @author maxid
 */
@Value
@Builder(toBuilder = true)
public class Mqtt3Connect {

    String      clientId;
    boolean     cleanSession;
    int         keepAliveSeconds;
    String      username;
    byte[]      password;
    MqttPublish willPublish;

}
