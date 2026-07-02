package plus.jmqx.client.mqtt.v5;

import plus.jmqx.client.mqtt.message.MqttPublishResult;
import plus.jmqx.client.mqtt.v5.message.Mqtt5Publish;

/**
 * MQTT 5 PUBLISH 的发布结果。
 *
 * @author maxid
 * @since 1.4.14
 */
public interface Mqtt5PublishResult extends MqttPublishResult {

    @Override
    Mqtt5Publish getPublish();

}
