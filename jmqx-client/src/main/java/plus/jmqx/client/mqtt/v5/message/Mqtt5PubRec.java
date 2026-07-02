package plus.jmqx.client.mqtt.v5.message;

import lombok.Value;

/**
 * MQTT 5.0 PUBREC。
 *
 * @author maxid
 */
@Value
public class Mqtt5PubRec {

    int  packetId;
    byte reasonCode;

}
