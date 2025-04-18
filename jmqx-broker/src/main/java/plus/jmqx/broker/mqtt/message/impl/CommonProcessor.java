package plus.jmqx.broker.mqtt.message.impl;

import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import plus.jmqx.broker.mqtt.channel.SessionStatus;
import plus.jmqx.broker.mqtt.channel.MqttSession;
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
 * PINGRESP、PINGREQ、DISCONNECT、PUBCOMP、PUBREC、PUBREL 消息流程处理
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
    public Mono<Void> process(MessageWrapper<MqttMessage> wrapper, MqttSession session, ContextView view) {
        ReceiveContext<?> context = view.get(ReceiveContext.class);
        MqttMessage message = wrapper.getMessage();
        MqttMessageIdVariableHeader header = (MqttMessageIdVariableHeader) message.variableHeader();
        switch (message.fixedHeader().messageType()) {
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
                    Ack ack = context.getTimeAckManager().getAck(session.generateId(MqttMessageType.PUBLISH, header.messageId()));
                    Optional.ofNullable(ack).ifPresent(Ack::stop);
                }).then(session.write(MqttMessageBuilder.publishRelMessage(header.messageId()), true));
            case PUBREL:
                return session.removeQos2Msg(header.messageId())
                        .map(msg -> {
                            TopicRegistry topicRegistry = context.getTopicRegistry();
                            MessageRegistry messageRegistry = context.getMessageRegistry();
                            Set<SubscribeTopic> subscribeTopics = topicRegistry.getSubscribesByTopic(msg.variableHeader().topicName(), msg.fixedHeader().qosLevel());
                            return Mono.when(subscribeTopics.stream()
                                            .filter(topic -> filterOfflineSession(topic.getMqttChannel(), messageRegistry, MessageUtils.wrapPublishMessage(msg, topic.getQoS(), topic.getMqttChannel().generateMessageId())))
                                            .map(topic -> topic.getMqttChannel().write(MessageUtils.wrapPublishMessage(msg, topic.getQoS(), topic.getMqttChannel().generateMessageId()), topic.getQoS().value() > 0))
                                            .collect(Collectors.toList()))
                                    .then(Mono.fromRunnable(() -> Optional.ofNullable(context.getTimeAckManager().getAck(session.generateId(MqttMessageType.PUBREC, header.messageId())))
                                            .ifPresent(Ack::stop)))
                                    .then(session.write(MqttMessageBuilder.buildPublishComp(header.messageId()), false));
                        }).orElseGet(() -> session.write(MqttMessageBuilder.buildPublishComp(header.messageId()), false));
            case PUBCOMP:
                return Mono.fromRunnable(() -> {
                    Ack ack = context.getTimeAckManager().getAck(session.generateId(MqttMessageType.PUBREL, header.messageId()));
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
     * @param session         {@link MqttSession} 会话
     * @param messageRegistry {@link MessageRegistry} 消息注册中心
     * @param mqttMessage     {@link MqttPublishMessage} MQTT 消息
     * @return boolean
     */
    private boolean filterOfflineSession(MqttSession session, MessageRegistry messageRegistry, MqttPublishMessage mqttMessage) {
        if (session.getStatus() == SessionStatus.ONLINE) {
            return true;
        } else {
            messageRegistry.saveSessionMessage(SessionMessage.of(session.getClientId(), mqttMessage));
            return false;
        }
    }
}
