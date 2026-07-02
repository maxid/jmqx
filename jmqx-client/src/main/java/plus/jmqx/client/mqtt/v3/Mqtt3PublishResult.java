package plus.jmqx.client.mqtt.v3;

import plus.jmqx.client.mqtt.message.MqttPublishResult;
import plus.jmqx.client.mqtt.v3.message.Mqtt3Publish;

/**
 * MQTT 3 PUBLISH 的发布结果。
 *
 * <p>QoS1/2 在收到 ACK 时完成；QoS0 在发送时完成。
 *
 * @author maxid
 */
public interface Mqtt3PublishResult extends MqttPublishResult {

    @Override
    Mqtt3Publish getPublish();
}
