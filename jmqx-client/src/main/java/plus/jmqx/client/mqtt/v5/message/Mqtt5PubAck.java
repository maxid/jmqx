package plus.jmqx.client.mqtt.v5.message;

import lombok.Value;

/**
 * MQTT 5.0 PUBACK。
 *
 * @author maxid
 */
@Value
public class Mqtt5PubAck {

    /**
     * 报文标识符
     */
    int  packetId;
    /**
     * 应答原因码
     */
    byte reasonCode;

}
