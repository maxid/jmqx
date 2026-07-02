package plus.jmqx.client.mqtt.v3.message;

import lombok.Builder;
import lombok.Value;
import plus.jmqx.client.mqtt.message.MqttTopicFilter;
import plus.jmqx.client.mqtt.message.QoS;

/**
 * MQTT 3.1.1 主题过滤器。
 *
 * @author maxid
 */
@Value
@Builder
public class Mqtt3TopicFilter implements MqttTopicFilter {

    String topicFilter;
    QoS    qos;

    @Override
    public QoS getQoS() {
        return qos;
    }

}
