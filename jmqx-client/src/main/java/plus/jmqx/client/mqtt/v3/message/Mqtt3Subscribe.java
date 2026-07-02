package plus.jmqx.client.mqtt.v3.message;

import lombok.Builder;
import lombok.Value;
import plus.jmqx.client.mqtt.message.MqttSubscribe;

import java.util.List;

/**
 * MQTT 3.1.1 SUBSCRIBE。
 *
 * @author maxid
 */
@Value
@Builder(toBuilder = true)
public class Mqtt3Subscribe implements MqttSubscribe {

    List<Mqtt3TopicFilter> topicFilters;
    int packetId;
}
