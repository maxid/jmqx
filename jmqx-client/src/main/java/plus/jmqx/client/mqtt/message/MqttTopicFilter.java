package plus.jmqx.client.mqtt.message;

/**
 * 版本无关的主题过滤器。
 *
 * @author maxid
 */
public interface MqttTopicFilter {

    String getTopicFilter();

    QoS getQoS();
}
