package plus.jmqx.client.mqtt.v3.message;

import lombok.Value;

/**
 * MQTT 3.1.1 PUBREL（发布释放）报文。
 *
 * <p>QoS 2 协议流程的第三步，由发送方发送给接收方，表示准备完成发布。
 *
 * @author maxid
 */
@Value
public class Mqtt3PubRel {

    /**
     * 报文标识符
     */
    int packetId;

}
