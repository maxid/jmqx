package plus.jmqx.broker.mqtt.message;

import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;

/**
 * 消息类型抽象
 *
 * @author maxid
 * @since 2025/9/24 15:36
 */
public interface MessageTypeWrapper<T extends MqttMessage> {
    MessageWrapper<T> getWrapper();
}
