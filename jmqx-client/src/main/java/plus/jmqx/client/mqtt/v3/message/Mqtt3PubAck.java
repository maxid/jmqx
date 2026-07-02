package plus.jmqx.client.mqtt.v3.message;

import lombok.Value;

/**
 * MQTT 3.1.1 PUBACK（发布确认）报文。
 *
 * <p>对 QoS 1 的 PUBLISH 确认回复。
 *
 * @author maxid
 */
@Value
public class Mqtt3PubAck {

    /**
     * 报文标识符
     */
    int packetId;

}
