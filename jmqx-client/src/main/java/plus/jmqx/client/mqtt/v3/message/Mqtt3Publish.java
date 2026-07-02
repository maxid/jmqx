package plus.jmqx.client.mqtt.v3.message;

import plus.jmqx.client.mqtt.message.MqttPublish;
import plus.jmqx.client.mqtt.message.QoS;

/**
 * MQTT 3.1.1 PUBLISH（不可变值类型）。
 *
 * @author maxid
 */
public interface Mqtt3Publish extends MqttPublish {

    /**
     * 确认该入站消息。对于 QoS1/2 引擎的 inbox 会在订阅者消费后发送 PUBACK/PUBREC；QoS0 为 no-op。
     * 对出站 PUBLISH 调用为 no-op。可安全多次调用。
     */
    @Override
    default void ack() {
    }

    static Mqtt3PublishBuilder builder() {
        return new Mqtt3PublishBuilder();
    }

    /** 可变 builder（impl 通过 {@link #toBuilder()} 复制）。 */
    Mqtt3PublishBuilder toBuilder();
}
