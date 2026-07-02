package plus.jmqx.client.mqtt.v3.message;

import lombok.Value;
import plus.jmqx.client.mqtt.message.MqttSubAck;
import plus.jmqx.client.mqtt.message.QoS;

import java.util.List;

/**
 * MQTT 3.1.1 SUBACK（订阅确认）报文。
 *
 * <p>包含服务端授予的 QoS 等级列表及对应的报文标识符。
 *
 * @author maxid
 */
@Value
public class Mqtt3SubAck implements MqttSubAck {

    /** 服务端授予的 QoS 等级列表 */
    List<QoS> grantedQos;
    /** 报文标识符 */
    int       packetId;

}
