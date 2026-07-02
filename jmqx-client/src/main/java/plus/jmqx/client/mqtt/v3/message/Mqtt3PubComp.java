package plus.jmqx.client.mqtt.v3.message;

import lombok.Value;

/**
 * MQTT 3.1.1 PUBCOMP（发布完成）报文。
 *
 * <p>QoS 2 协议流程的第四步，由接收方发送给发送方，确认发布完成。
 *
 * @author maxid
 */
@Value
public class Mqtt3PubComp {

    /**
     * 报文标识符
     */
    int packetId;

}
