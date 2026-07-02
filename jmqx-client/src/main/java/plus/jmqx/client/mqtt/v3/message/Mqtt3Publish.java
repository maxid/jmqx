package plus.jmqx.client.mqtt.v3.message;

import plus.jmqx.client.mqtt.message.MqttPublish;

/**
 * MQTT 3.1.1 PUBLISH（不可变值类型）。
 *
 * @author maxid
 * @since 1.4.14
 */
public interface Mqtt3Publish extends MqttPublish {

    /**
     * {@inheritDoc}
     */
    @Override
    default void ack() {
    }

    /**
     * @return 新的 {@link Mqtt3PublishBuilder}
     */
    static Mqtt3PublishBuilder builder() {
        return new Mqtt3PublishBuilder();
    }

    /**
     * @return 基于当前值的 builder 副本
     */
    Mqtt3PublishBuilder toBuilder();

}
