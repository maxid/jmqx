package plus.jmqx.client.mqtt.v3;

import plus.jmqx.client.mqtt.message.MqttPublishResult;
import plus.jmqx.client.mqtt.v3.message.Mqtt3Publish;

/**
 * MQTT 3 PUBLISH 的发布结果。
 *
 * @author maxid
 * @since 1.4.14
 */
public interface Mqtt3PublishResult extends MqttPublishResult {

    @Override
    Mqtt3Publish getPublish();

}
