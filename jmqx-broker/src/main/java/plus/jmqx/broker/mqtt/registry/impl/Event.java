package plus.jmqx.broker.mqtt.registry.impl;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import plus.jmqx.broker.cluster.ClusterSession;
import plus.jmqx.broker.mqtt.channel.SessionStatus;
import plus.jmqx.broker.mqtt.channel.MqttSession;
import plus.jmqx.broker.mqtt.context.ReceiveContext;
import plus.jmqx.broker.mqtt.message.MessageWrapper;
import plus.jmqx.broker.mqtt.message.MqttMessageBuilder;
import plus.jmqx.broker.mqtt.message.system.SessionStatusMessage;
import plus.jmqx.broker.mqtt.util.JacksonUtil;

/**
 * MQTT 设备事件
 *
 * @author maxid
 * @since 2025/4/18 09:19
 */
public enum Event {

    /**
     * 连接事件
     */
    CONNECT {
        private static final String CONNECT_TOPIC = "$event/connect";

        @Override
        public void sender(MqttSession session, Object body, ReceiveContext<?> context) {
            MqttPublishMessage mqttPublishMessage =
                    MqttMessageBuilder.publishMessage(false, MqttQoS.AT_MOST_ONCE, 0, CONNECT_TOPIC, writeBody(session, body));
            write(context, session, mqttPublishMessage);
        }

        @Override
        public ByteBuf writeBody(MqttSession mqttChannel, Object body) {
            return PooledByteBufAllocator.DEFAULT
                    .directBuffer().writeBytes(JacksonUtil.bean2Json(new SessionStatusMessage(
                            mqttChannel.getClientId(),
                            System.currentTimeMillis(),
                            mqttChannel.getUsername(),
                            SessionStatus.ONLINE)).getBytes());
        }

    },
    /**
     * 关闭事件
     */
    CLOSE {
        private static final String CLOSE_TOPIC = "$event/close";

        @Override
        public void sender(MqttSession session, Object body, ReceiveContext<?> context) {
            MqttPublishMessage message = MqttMessageBuilder.publishMessage(
                    false,
                    MqttQoS.AT_MOST_ONCE,
                    0,
                    CLOSE_TOPIC,
                    writeBody(session, body)
            );
            write(context, session, message);
        }

        @Override
        public ByteBuf writeBody(MqttSession mqttChannel, Object body) {
            return PooledByteBufAllocator.DEFAULT
                    .directBuffer().writeBytes(JacksonUtil.bean2Json(new SessionStatusMessage(
                            mqttChannel.getClientId(),
                            System.currentTimeMillis(),
                            mqttChannel.getUsername(),
                            SessionStatus.OFFLINE)).getBytes());
        }
    };

    /**
     * 发送事件消息
     *
     * @param session {@link MqttSession } 会话
     * @param body    {@link Object } 事件消息
     * @param context {@link ReceiveContext } 上下文
     */
    public abstract void sender(MqttSession session, Object body, ReceiveContext<?> context);


    /**
     * 构建事件消息
     *
     * @param mqttChannel {@link MqttSession } 会话
     * @param body        {@link Object } 事件消息
     * @return ByteBuf 缓冲
     */
    public abstract ByteBuf writeBody(MqttSession mqttChannel, Object body);

    /**
     * 分发事件消息
     *
     * @param context 上下文
     * @param session 会话
     * @param message 事件消息
     */
    public void write(ReceiveContext<?> context, MqttSession session, MqttMessage message) {
        context.getMessageAdapter().dispatch(
                ClusterSession.wrapClientId(session.getClientId()),
                new MessageWrapper<>(message, System.currentTimeMillis(), Boolean.FALSE),
                context
        );
        if (message instanceof MqttPublishMessage) {
            ((MqttPublishMessage) message).release();
        }
    }
}
