package plus.jmqx.broker.mqtt.message;

import io.netty.handler.codec.mqtt.MqttMessage;

/**
 * MQTT 消息处理器抽象，命名空间支持
 *
 * @author maxid
 * @since 2026/1/11 16:51
 */
public abstract class NamespceMessageProcessor<T extends MqttMessage> implements MessageProcessor<T> {

    private String namespace;

    @Override
    public String getNamespace() {
        return namespace;
    }

    @Override
    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }
}
