package plus.jmqx.broker.mqtt.message;

import io.netty.handler.codec.mqtt.MqttMessage;

/**
 * 消息类型抽象
 *
 * @author maxid
 * @since 2025/9/24 15:36
 */
public interface MessageTypeWrapper<T extends MqttMessage> {

    /**
     * 获取消息包装器
     *
     * @return 消息包装器
     */
    MessageWrapper<T> getWrapper();

}
