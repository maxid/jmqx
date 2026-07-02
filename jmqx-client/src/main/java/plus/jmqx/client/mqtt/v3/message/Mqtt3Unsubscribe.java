package plus.jmqx.client.mqtt.v3.message;

import lombok.Builder;
import lombok.Value;
import plus.jmqx.client.mqtt.message.MqttUnsubscribe;

import java.util.List;

/**
 * MQTT 3.1.1 UNSUBSCRIBE。
 *
 * @author maxid
 */
@Value
@Builder(toBuilder = true)
public class Mqtt3Unsubscribe implements MqttUnsubscribe {

    List<String> topicFilters;
    int          packetId;

}
