package plus.jmqx.client.mqtt.v5.message;

import lombok.Builder;
import lombok.Value;
import plus.jmqx.client.mqtt.message.MqttTopicFilter;
import plus.jmqx.client.mqtt.message.QoS;

/**
 * MQTT 5.0 主题过滤器。
 *
 * @author maxid
 */
@Value
@Builder
public class Mqtt5TopicFilter implements MqttTopicFilter {

    /** 主题过滤器表达式 */
    String topicFilter;
    /** 请求的 QoS 等级 */
    QoS    qos;

    @Override
    public QoS getQoS() {
        return qos;
    }

}
