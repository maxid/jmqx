package plus.jmqx.broker.mqtt.message.impl;

import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import plus.jmqx.broker.mqtt.channel.ChannelStatus;
import plus.jmqx.broker.mqtt.channel.MqttChannel;
import plus.jmqx.broker.mqtt.context.ReceiveContext;
import plus.jmqx.broker.mqtt.message.MessageProcessor;
import plus.jmqx.broker.mqtt.message.SessionMessage;
import plus.jmqx.broker.mqtt.registry.MessageRegistry;
import plus.jmqx.broker.mqtt.message.MessageWrapper;
import plus.jmqx.broker.mqtt.message.MqttMessageBuilder;
import plus.jmqx.broker.mqtt.retry.Ack;
import plus.jmqx.broker.mqtt.topic.SubscribeTopic;
import plus.jmqx.broker.mqtt.registry.TopicRegistry;
import plus.jmqx.broker.mqtt.util.MessageUtils;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.util.context.ContextView;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 尽量简洁一句描述
 *
 * @author maxid
 * @since 2025/4/9 15:28
 */
public class CommonProcessor implements MessageProcessor<MqttMessage> {

    private static final List<MqttMessageType> MESSAGE_TYPES = new ArrayList<>();

    static {
        MESSAGE_TYPES.add(MqttMessageType.PINGRESP);
        MESSAGE_TYPES.add(MqttMessageType.PINGREQ);
        MESSAGE_TYPES.add(MqttMessageType.DISCONNECT);
        MESSAGE_TYPES.add(MqttMessageType.PUBCOMP);
        MESSAGE_TYPES.add(MqttMessageType.PUBREC);
        MESSAGE_TYPES.add(MqttMessageType.PUBREL);
    }

    @Override
    public List<MqttMessageType> getMqttMessageTypes() {
        return MESSAGE_TYPES;
    }

    @Override
    public Mono<Void> process(MessageWrapper<MqttMessage> message, MqttChannel session, ContextView view) {
        ReceiveContext<?> context = view.get(ReceiveContext.class);
        MqttMessage mqttMessage = message.getMessage();
        MqttMessageIdVariableHeader header = (MqttMessageIdVariableHeader) mqttMessage.variableHeader();
        int messageId = header.messageId();
        switch (mqttMessage.fixedHeader().messageType()) {
            case PINGREQ:
                return session.write(MqttMessageBuilder.pongMessage(), false);
            case DISCONNECT:
                return Mono.fromRunnable(() -> {
                    // MetricManagerHolder.metricManager.getMetricRegistry().getMetricCounter(CounterType.DIS_CONNECT_EVENT).increment();
                    session.setWill(null);
                    Connection connection;
                    if (!(connection = session.getConnection()).isDisposed()) {
                        connection.dispose();
                    }
                });
            case PUBREC:
                return Mono.fromRunnable(() -> {
                    Ack ack = context.getTimeAckManager().getAck(session.generateId(MqttMessageType.PUBLISH, messageId));
                    Optional.ofNullable(ack).ifPresent(Ack::stop);
                }).then(session.write(MqttMessageBuilder.publishRelMessage(messageId), true));
            case PUBREL:
                return session.removeQos2Msg(messageId)
                        .map(msg -> {
                            TopicRegistry topicRegistry = context.getTopicRegistry();
                            MessageRegistry messageRegistry = context.getMessageRegistry();
                            Set<SubscribeTopic> subscribeTopics = topicRegistry.getSubscribesByTopic(msg.variableHeader().topicName(), msg.fixedHeader().qosLevel());
                            return Mono.when(subscribeTopics.stream()
                                            .filter(topic -> filterOfflineSession(topic.getMqttChannel(), messageRegistry, MessageUtils.wrapPublishMessage(msg, topic.getQoS(), topic.getMqttChannel().generateMessageId())))
                                            .map(topic -> topic.getMqttChannel().write(MessageUtils.wrapPublishMessage(msg, topic.getQoS(), topic.getMqttChannel().generateMessageId()), topic.getQoS().value() > 0))
                                            .collect(Collectors.toList()))
                                    .then(Mono.fromRunnable(() -> Optional.ofNullable(context.getTimeAckManager().getAck(session.generateId(MqttMessageType.PUBREC, messageId)))
                                            .ifPresent(Ack::stop)))
                                    .then(session.write(MqttMessageBuilder.buildPublishComp(messageId), false));
                        }).orElseGet(() -> session.write(MqttMessageBuilder.buildPublishComp(messageId), false));
            case PUBCOMP:
                return Mono.fromRunnable(() -> {
                    Ack ack = context.getTimeAckManager().getAck(session.generateId(MqttMessageType.PUBREL, messageId));
                    Optional.ofNullable(ack).ifPresent(Ack::stop);
                });
            case PINGRESP:
            default:
                return Mono.empty();
        }
    }

    /**
     * 过滤离线会话消息
     *
     * @param session         {@link MqttChannel} 会话
     * @param messageRegistry {@link MessageRegistry} 消息注册中心
     * @param mqttMessage     {@link MqttPublishMessage} MQTT 消息
     * @return boolean
     */
    private boolean filterOfflineSession(MqttChannel session, MessageRegistry messageRegistry, MqttPublishMessage mqttMessage) {
        if (session.getStatus() == ChannelStatus.ONLINE) {
            return true;
        } else {
            messageRegistry.saveSessionMessage(SessionMessage.of(session.getClientId(), mqttMessage));
            return false;
        }
    }
}
