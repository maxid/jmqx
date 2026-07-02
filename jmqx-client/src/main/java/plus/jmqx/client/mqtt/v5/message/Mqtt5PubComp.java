package plus.jmqx.client.mqtt.v5.message;

import lombok.Value;

/**
 * MQTT 5.0 PUBCOMP。
 *
 * @author maxid
 */
@Value
public class Mqtt5PubComp {

    /**
     * 报文标识符
     */
    int  packetId;
    /**
     * 完成原因码
     */
    byte reasonCode;

}
