package plus.jmqx.broker.mqtt.message;

import io.netty.handler.codec.mqtt.*;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import plus.jmqx.broker.mqtt.channel.MqttSession;
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
     * 获取消息类型
     *
     * @return 消息类型
     */
    Class<?> getMessageType();

    /**
     * 处理 MQTT 消息, 并添加上下文
     *
     * @param message {@link MessageWrapper} 消息
     * @param session {@link MqttSession} 消息会话
     * @return Mono
     */
    default Mono<Void> process(MessageWrapper<T> message, MqttSession session) {
        return Mono.deferContextual(view -> {
            this.process(message, session, view);
            return Mono.empty();
        });
    }

    /**
     * 处理 MQTT 消息, 并添加上下文
     *
     * @param wrapper {@link MessageWrapper} 消息
     * @param session {@link MqttSession} 消息会话
     * @param view    {@link ContextView} 上下文视图
     */
    void process(MessageWrapper<T> wrapper, MqttSession session, ContextView view);

    @Data
    @RequiredArgsConstructor
    class CommonMessageType implements MessageTypeWrapper<MqttMessage> {
        private final MessageWrapper<MqttMessage> wrapper;

        public static CommonMessageType of(MessageWrapper<MqttMessage> wrapper) {
            return new CommonMessageType(wrapper);
        }
    }

    @Data
    @RequiredArgsConstructor
    class ConnectAckMessageType implements MessageTypeWrapper<MqttConnAckMessage> {
        private final MessageWrapper<MqttConnAckMessage> wrapper;

        public static ConnectAckMessageType of(MessageWrapper<MqttMessage> wrapper) {
            return new ConnectAckMessageType((MessageWrapper) wrapper);
        }
    }

    @Data
    @RequiredArgsConstructor
    class ConnectMessageType implements MessageTypeWrapper<MqttConnectMessage> {
        private final MessageWrapper<MqttConnectMessage> wrapper;

        public static ConnectMessageType of(MessageWrapper<MqttMessage> wrapper) {
            return new ConnectMessageType((MessageWrapper) wrapper);
        }
    }

    @Data
    @RequiredArgsConstructor
    class PublishAckMessageType implements MessageTypeWrapper<MqttPubAckMessage> {
        private final MessageWrapper<MqttPubAckMessage> wrapper;

        public static PublishAckMessageType of(MessageWrapper<MqttMessage> wrapper) {
            return new PublishAckMessageType((MessageWrapper) wrapper);
        }
    }

    @Data
    @RequiredArgsConstructor
    class PublishMessageType implements MessageTypeWrapper<MqttPublishMessage> {
        private final MessageWrapper<MqttPublishMessage> wrapper;

        public static PublishMessageType of(MessageWrapper<MqttMessage> wrapper) {
            return new PublishMessageType((MessageWrapper) wrapper);
        }
    }

    @Data
    @RequiredArgsConstructor
    class SubscribeAckMessageType implements MessageTypeWrapper<MqttSubAckMessage> {
        private final MessageWrapper<MqttSubAckMessage> wrapper;

        public static SubscribeAckMessageType of(MessageWrapper<MqttMessage> wrapper) {
            return new SubscribeAckMessageType((MessageWrapper) wrapper);
        }
    }

    @Data
    @RequiredArgsConstructor
    class SubscribeMessageType implements MessageTypeWrapper<MqttSubscribeMessage> {
        private final MessageWrapper<MqttSubscribeMessage> wrapper;

        public static SubscribeMessageType of(MessageWrapper<MqttMessage> wrapper) {
            return new SubscribeMessageType((MessageWrapper) wrapper);
        }
    }

    @Data
    @RequiredArgsConstructor
    class UnsubscribeAckMessageType implements MessageTypeWrapper<MqttUnsubAckMessage> {
        private final MessageWrapper<MqttUnsubAckMessage> wrapper;

        public static UnsubscribeAckMessageType of(MessageWrapper<MqttMessage> wrapper) {
            return new UnsubscribeAckMessageType((MessageWrapper) wrapper);
        }
    }

    @Data
    @RequiredArgsConstructor
    class UnsubscribeMessageType implements MessageTypeWrapper<MqttUnsubscribeMessage> {
        private final MessageWrapper<MqttUnsubscribeMessage> wrapper;

        public static UnsubscribeMessageType of(MessageWrapper<MqttMessage> wrapper) {
            return new UnsubscribeMessageType((MessageWrapper) wrapper);
        }
    }
}
