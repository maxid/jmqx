package plus.jmqx.broker.mqtt.message;

import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import plus.jmqx.broker.mqtt.channel.MqttChannel;
import reactor.core.publisher.Mono;
import reactor.util.context.ContextView;

import java.util.List;

/**
 * MQTT 消息处理器抽象
 *
 * @author maxid
 * @since 2025/4/8 17:41
 */
public interface MessageProcessor<T extends MqttMessage> {

    /**
     * 获取消息处理器适配的消息类型
     *
     * @return 消息类型集合
     */
    List<MqttMessageType> getMqttMessageTypes();

    /**
     * 处理 MQTT 消息, 并添加上下文
     *
     * @param message {@link MessageWrapper} 消息
     * @param session {@link MqttChannel} 消息会话
     * @return Mono
     */
    default Mono<Void> process(MessageWrapper<T> message, MqttChannel session) {
        return Mono.deferContextual(view -> this.process(message, session, view));
    }

    /**
     * 处理 MQTT 消息, 并添加上下文
     *
     * @param message {@link MessageWrapper} 消息
     * @param session {@link MqttChannel} 消息会话
     * @param view    {@link ContextView} 上下文视图
     * @return Mono
     */
    Mono<Void> process(MessageWrapper<T> message, MqttChannel session, ContextView view);
}
