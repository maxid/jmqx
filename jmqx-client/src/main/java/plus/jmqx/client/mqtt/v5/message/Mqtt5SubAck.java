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

    /** 授予的 QoS 列表 */
    List<QoS>  grantedQos;
    /** 原因码列表 */
    List<Byte> reasonCodes;
    /** 报文标识符 */
    int        packetId;

}
