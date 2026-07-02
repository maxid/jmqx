package plus.jmqx.client.mqtt.v5.message;

import lombok.Builder;
import lombok.Value;
import plus.jmqx.client.mqtt.message.MqttUnsubscribe;

import java.util.List;

/**
 * MQTT 5.0 UNSUBSCRIBE。
 *
 * @author maxid
 */
@Value
@Builder(toBuilder = true)
public class Mqtt5Unsubscribe implements MqttUnsubscribe {

    List<String> topicFilters;
    int packetId;
}
