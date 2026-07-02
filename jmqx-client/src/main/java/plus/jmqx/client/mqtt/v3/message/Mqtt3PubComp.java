package plus.jmqx.client.mqtt.v3.message;

import lombok.Value;

/**
 * MQTT 3.1.1 PUBCOMP。
 *
 * @author maxid
 */
@Value
public class Mqtt3PubComp {

    int packetId;

}
