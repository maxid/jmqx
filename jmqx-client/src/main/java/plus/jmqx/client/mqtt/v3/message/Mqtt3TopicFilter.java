package plus.jmqx.client.mqtt.v3.message;

import lombok.Builder;
import lombok.Value;
import plus.jmqx.client.mqtt.message.MqttTopicFilter;
import plus.jmqx.client.mqtt.message.QoS;

/**
 * MQTT 3.1.1 主题过滤器。
 *
 * <p>包含主题过滤表达式及对应的 QoS 等级。
 *
 * @author maxid
 */
@Value
@Builder
public class Mqtt3TopicFilter implements MqttTopicFilter {

    /**
     * 主题过滤表达式（支持通配符 + 和 #）
     */
    String topicFilter;
    /**
     * 请求的 QoS 等级
     */
    QoS    qos;

    @Override
    public QoS getQoS() {
        return qos;
    }

}
