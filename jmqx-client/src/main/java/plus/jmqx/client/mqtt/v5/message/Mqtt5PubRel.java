package plus.jmqx.client.mqtt.v5.message;

import lombok.Value;

/**
 * MQTT 5.0 PUBREL。
 *
 * @author maxid
 */
@Value
public class Mqtt5PubRel {

    /**
     * 报文标识符
     */
    int  packetId;
    /**
     * 释放原因码
     */
    byte reasonCode;

}
