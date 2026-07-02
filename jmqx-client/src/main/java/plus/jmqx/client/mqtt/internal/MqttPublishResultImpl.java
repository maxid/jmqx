package plus.jmqx.client.mqtt.internal;

import plus.jmqx.client.mqtt.message.MqttPublish;
import plus.jmqx.client.mqtt.message.MqttPublishResult;

/**
 * 版本无关的 {@link MqttPublishResult} 实现。
 *
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
