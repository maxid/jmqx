package plus.jmqx.broker.mqtt.message;

import io.netty.handler.codec.mqtt.*;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import plus.jmqx.broker.mqtt.channel.MqttSession;
import plus.jmqx.broker.mqtt.context.ContextHolder;
import plus.jmqx.broker.mqtt.context.NamespaceContextHolder;
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
     * 获取命名空间
     *
     * @return 命名空间
     */
    String getNamespace();

    /**
     * 设置命名空间
     *
     * @param namespace 命名空间
     */
    void setNamespace(String namespace);

    /**
     * 获取上下文持有器
     *
     * @return 上下文持有器
     */
    default ContextHolder contextHolder() {
        return NamespaceContextHolder.get(getNamespace());
    }

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
     * 处理 MQTT 消息并注入上下文
     *
     * @param message 消息包装
     * @param session 会话
     * @return 响应 Mono
     */
    default Mono<Void> process(MessageWrapper<T> message, MqttSession session) {
        return Mono.deferContextual(view -> {
            this.process(message, session, view);
            return Mono.empty();
        });
    }

    /**
     * 处理 MQTT 消息并注入上下文
     *
     * @param wrapper 消息包装
     * @param session 会话
     * @param view    上下文视图
     */
    void process(MessageWrapper<T> wrapper, MqttSession session, ContextView view);

    /**
     * 通用消息类型
     */
    @Data
    @RequiredArgsConstructor
    class CommonMessageType implements MessageTypeWrapper<MqttMessage> {
        private final MessageWrapper<MqttMessage> wrapper;

        /**
         * 创建通用消息类型包装
         *
         * @param wrapper 消息包装
         * @return 包装对象
         */
        public static CommonMessageType of(MessageWrapper<MqttMessage> wrapper) {
            return new CommonMessageType(wrapper);
        }
    }

    /**
     * 连接确认消息类型
     */
    @Data
    @RequiredArgsConstructor
    class ConnectAckMessageType implements MessageTypeWrapper<MqttConnAckMessage> {
        private final MessageWrapper<MqttConnAckMessage> wrapper;

        /**
         * 创建连接确认消息类型包装
         *
         * @param wrapper 消息包装
         * @return 包装对象
         */
        public static ConnectAckMessageType of(MessageWrapper<MqttMessage> wrapper) {
            return new ConnectAckMessageType((MessageWrapper) wrapper);
        }
    }

    /**
     * 连接消息类型
     */
    @Data
    @RequiredArgsConstructor
    class ConnectMessageType implements MessageTypeWrapper<MqttConnectMessage> {
        private final MessageWrapper<MqttConnectMessage> wrapper;

        /**
         * 创建连接消息类型包装
         *
         * @param wrapper 消息包装
         * @return 包装对象
         */
        public static ConnectMessageType of(MessageWrapper<MqttMessage> wrapper) {
            return new ConnectMessageType((MessageWrapper) wrapper);
        }
    }

    /**
     * 发布确认消息类型
     */
    @Data
    @RequiredArgsConstructor
    class PublishAckMessageType implements MessageTypeWrapper<MqttPubAckMessage> {
        private final MessageWrapper<MqttPubAckMessage> wrapper;

        /**
         * 创建发布确认消息类型包装
         *
         * @param wrapper 消息包装
         * @return 包装对象
         */
        public static PublishAckMessageType of(MessageWrapper<MqttMessage> wrapper) {
            return new PublishAckMessageType((MessageWrapper) wrapper);
        }
    }

    /**
     * 发布消息类型
     */
    @Data
    @RequiredArgsConstructor
    class PublishMessageType implements MessageTypeWrapper<MqttPublishMessage> {
        private final MessageWrapper<MqttPublishMessage> wrapper;

        /**
         * 创建发布消息类型包装
         *
         * @param wrapper 消息包装
         * @return 包装对象
         */
        public static PublishMessageType of(MessageWrapper<MqttMessage> wrapper) {
            return new PublishMessageType((MessageWrapper) wrapper);
        }
    }

    /**
     * 订阅确认消息类型
     */
    @Data
    @RequiredArgsConstructor
    class SubscribeAckMessageType implements MessageTypeWrapper<MqttSubAckMessage> {
        private final MessageWrapper<MqttSubAckMessage> wrapper;

        /**
         * 创建订阅确认消息类型包装
         *
         * @param wrapper 消息包装
         * @return 包装对象
         */
        public static SubscribeAckMessageType of(MessageWrapper<MqttMessage> wrapper) {
            return new SubscribeAckMessageType((MessageWrapper) wrapper);
        }
    }

    /**
     * 订阅消息类型
     */
    @Data
    @RequiredArgsConstructor
    class SubscribeMessageType implements MessageTypeWrapper<MqttSubscribeMessage> {
        private final MessageWrapper<MqttSubscribeMessage> wrapper;

        /**
         * 创建订阅消息类型包装
         *
         * @param wrapper 消息包装
         * @return 包装对象
         */
        public static SubscribeMessageType of(MessageWrapper<MqttMessage> wrapper) {
            return new SubscribeMessageType((MessageWrapper) wrapper);
        }
    }

    /**
     * 去订阅确认消息类型
     */
    @Data
    @RequiredArgsConstructor
    class UnsubscribeAckMessageType implements MessageTypeWrapper<MqttUnsubAckMessage> {
        private final MessageWrapper<MqttUnsubAckMessage> wrapper;

        /**
         * 创建去订阅确认消息类型包装
         *
         * @param wrapper 消息包装
         * @return 包装对象
         */
        public static UnsubscribeAckMessageType of(MessageWrapper<MqttMessage> wrapper) {
            return new UnsubscribeAckMessageType((MessageWrapper) wrapper);
        }
    }

    /**
     * 去订阅消息类型
     */
    @Data
    @RequiredArgsConstructor
    class UnsubscribeMessageType implements MessageTypeWrapper<MqttUnsubscribeMessage> {
        private final MessageWrapper<MqttUnsubscribeMessage> wrapper;

        /**
         * 创建去订阅消息类型包装
         *
         * @param wrapper 消息包装
         * @return 包装对象
         */
        public static UnsubscribeMessageType of(MessageWrapper<MqttMessage> wrapper) {
            return new UnsubscribeMessageType((MessageWrapper) wrapper);
        }
    }

}
