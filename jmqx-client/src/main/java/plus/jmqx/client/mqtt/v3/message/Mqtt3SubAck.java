package plus.jmqx.client.mqtt.v3.message;

import lombok.Value;
import plus.jmqx.client.mqtt.message.MqttSubAck;
import plus.jmqx.client.mqtt.message.QoS;

import java.util.List;

/**
 * MQTT 3.1.1 SUBACK。
 *
 * @author maxid
 */
@Value
public class Mqtt3SubAck implements MqttSubAck {

    List<QoS> grantedQos;
    int packetId;
}
