package plus.jmqx.client.mqtt.v5.message;

import lombok.Builder;
import lombok.Value;
import plus.jmqx.client.mqtt.message.MqttSubscribe;

import java.util.List;

/**
 * MQTT 5.0 SUBSCRIBE。
 *
 * @author maxid
 */
@Value
@Builder(toBuilder = true)
public class Mqtt5Subscribe implements MqttSubscribe {

    List<Mqtt5TopicFilter> topicFilters;
    int packetId;
}
