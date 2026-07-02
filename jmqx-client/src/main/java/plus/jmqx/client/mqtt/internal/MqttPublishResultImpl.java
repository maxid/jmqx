package plus.jmqx.client.mqtt.internal;

import plus.jmqx.client.mqtt.message.MqttPublish;
import plus.jmqx.client.mqtt.message.MqttPublishResult;

/**
 * 版本无关的 {@link MqttPublishResult} 实现。
 *
 * @param publish 发布的 MQTT 消息
 * @param error   发布过程中的错误（成功时为 null）
 * @author maxid
 */
public record MqttPublishResultImpl(MqttPublish publish, Throwable error) implements MqttPublishResult {

    @Override
    public MqttPublish getPublish() {
        return publish;
    }

    @Override
    public Throwable getError() {
        return error;
    }

}
