package plus.jmqx.client.mqtt.v5.message;

import lombok.Value;

/**
 * MQTT 5.0 PUBCOMP。
 *
 * @author maxid
 */
@Value
public class Mqtt5PubComp {

    int packetId;
    byte reasonCode;
}
