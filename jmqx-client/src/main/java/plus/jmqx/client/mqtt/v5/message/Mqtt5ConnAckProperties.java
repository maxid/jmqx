package plus.jmqx.client.mqtt.v5.message;

import lombok.Builder;
import lombok.Value;

/**
 * MQTT 5.0 CONNACK 属性。
 *
 * @author maxid
 */
@Value
@Builder
public class Mqtt5ConnAckProperties {

    int receiveMaximum;
    int serverKeepAlive;
    long sessionExpiryInterval;
    String responseInformation;
    String serverReference;
    String assignedClientIdentifier;
    boolean maximumPacketSizePresent;
    int maximumPacketSize;
}
