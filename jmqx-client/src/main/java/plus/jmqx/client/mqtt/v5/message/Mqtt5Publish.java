package plus.jmqx.client.mqtt.v5.message;

import plus.jmqx.client.mqtt.message.MqttPublish;

/**
 * MQTT 5.0 PUBLISH（不可变值类型）。
 *
 * @author maxid
 * @since 1.4.14
 */
public interface Mqtt5Publish extends MqttPublish {

    /**
     * 获取 MQTT 5 PUBLISH 属性。
     *
     * @return MQTT 5 PUBLISH 属性
     */
    Mqtt5PublishProperties getProperties();

    /**
     * {@inheritDoc}
     */
    @Override
    default void ack() {
    }

    /**
     * 创建新的 {@link Mqtt5PublishBuilder}。
     *
     * @return 新的 {@link Mqtt5PublishBuilder}
     */
    static Mqtt5PublishBuilder builder() {
        return new Mqtt5PublishBuilder();
    }

    /**
     * 返回基于当前值的 builder 副本。
     *
     * @return 基于当前值的 builder 副本
     */
    Mqtt5PublishBuilder toBuilder();

}
