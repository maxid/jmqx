package plus.jmqx.client.mqtt.v3.message;

import lombok.Value;

/**
 * MQTT 3.1.1 PUBREC（发布收到）报文。
 *
 * <p>QoS 2 协议流程的第二步，由接收方发送给发送方，表示已收到 PUBLISH。
 *
 * @author maxid
 */
@Value
public class Mqtt3PubRec {

    /**
     * 报文标识符
     */
    int packetId;

}
