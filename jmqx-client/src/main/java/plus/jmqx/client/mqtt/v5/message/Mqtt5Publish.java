package plus.jmqx.client.mqtt.v5.message;

import plus.jmqx.client.mqtt.message.MqttPublish;

/**
 * MQTT 5.0 PUBLISH（不可变值类型）。
 *
 * @author maxid
 */
public interface Mqtt5Publish extends MqttPublish {

    Mqtt5PublishProperties getProperties();

    @Override
    default void ack() {
    }

    static Mqtt5PublishBuilder builder() {
        return new Mqtt5PublishBuilder();
    }

    Mqtt5PublishBuilder toBuilder();
}
