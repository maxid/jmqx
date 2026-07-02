package plus.jmqx.client.mqtt.v3.message;

import plus.jmqx.client.mqtt.message.MqttPublish;

/**
 * MQTT 3.1.1 PUBLISH 报文（不可变值类型）。
 *
 * <p>表示一条 MQTT 发布消息，包含主题、负载、QoS、保留标志等属性。
 *
 * @author maxid
 * @since 1.4.14
 */
public interface Mqtt3Publish extends MqttPublish {

    /**
     * 确认消息已消费。MQTT 3.1.1 中无需显式确认，默认为空实现。
     */
    @Override
    default void ack() {
    }

    /**
     * 创建新的 {@link Mqtt3PublishBuilder}。
     *
     * @return 新的 builder
     */
    static Mqtt3PublishBuilder builder() {
        return new Mqtt3PublishBuilder();
    }

    /**
     * 基于当前值创建 builder 副本。
     *
     * @return 基于当前值的 builder 副本
     */
    Mqtt3PublishBuilder toBuilder();

}
