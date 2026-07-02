package plus.jmqx.client.mqtt.v5.message;

import lombok.Value;
import plus.jmqx.client.mqtt.message.MqttSubAck;
import plus.jmqx.client.mqtt.message.QoS;

import java.util.List;

/**
 * MQTT 5.0 SUBACK。
 *
 * @author maxid
 */
@Value
public class Mqtt5SubAck implements MqttSubAck {

    List<QoS> grantedQos;
    List<Byte> reasonCodes;
    int packetId;
}
