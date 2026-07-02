package plus.jmqx.client.mqtt.message;

/**
 * 版本无关的主题过滤器。
 *
 * @author maxid
 * @since 1.4.14
 */
public interface MqttTopicFilter {

    /**
     * 获取主题过滤器字符串
     *
     * @return 主题过滤器字符串（可含 {@code +} / {@code #} 通配符）
     */
    String getTopicFilter();

    /**
     * 获取请求的 QoS 等级
     *
     * @return 请求的 QoS 等级
     */
    QoS getQoS();

}
