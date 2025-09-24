package plus.jmqx.broker.mqtt.message.impl;

import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import lombok.extern.slf4j.Slf4j;
import plus.jmqx.broker.mqtt.channel.SessionStatus;
import plus.jmqx.broker.mqtt.channel.MqttSession;
import plus.jmqx.broker.mqtt.context.ContextHolder;
import plus.jmqx.broker.mqtt.context.ReceiveContext;
import plus.jmqx.broker.mqtt.message.*;
import plus.jmqx.broker.mqtt.message.dispatch.DisconnectMessage;
import plus.jmqx.broker.mqtt.registry.MessageRegistry;
import plus.jmqx.broker.mqtt.retry.Ack;
import plus.jmqx.broker.mqtt.topic.SubscribeTopic;
import plus.jmqx.broker.mqtt.registry.TopicRegistry;
import plus.jmqx.broker.mqtt.util.MessageUtils;
import reactor.netty.Connection;
import reactor.util.context.ContextView;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * PINGRESP、PINGREQ、DISCONNECT、PUBCOMP、PUBREC、PUBREL 消息流程处理
 *
 * @author maxid
 * @since 2025/4/9 15:28
 */
@Slf4j
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
    public Class<CommonMessageType> getMessageType() {
        return CommonMessageType.class;
    }

    @Override
    public void process(MessageWrapper<MqttMessage> wrapper, MqttSession session, ContextView view) {
        ReceiveContext<?> context = view.get(ReceiveContext.class);
        MqttMessage message = wrapper.getMessage();
        switch (message.fixedHeader().messageType()) {
            case PINGREQ:
                session.write(MqttMessageBuilder.pongMessage(), false);
                break;
            case DISCONNECT:
                // MetricManagerHolder.metricManager.getMetricRegistry().getMetricCounter(CounterType.DIS_CONNECT_EVENT).increment();
                session.setWill(null);
                Connection connection;
                if (!(connection = session.getConnection()).isDisposed()) {
                    connection.dispose();
                }
                context.dispatch(d -> d.onDisconnect(DisconnectMessage.builder()
                                .clientId(session.getClientId())
                                .username(session.getUsername())
                                .build())
                        .subscribeOn(ContextHolder.getDispatchScheduler())
                        .subscribe());
                break;
            case PUBREC:
                // QoS 2 step 1
                MqttMessageIdVariableHeader header1 = (MqttMessageIdVariableHeader) message.variableHeader();
                Ack ack1 = context.getTimeAckManager().getAck(session.generateId(MqttMessageType.PUBLISH, header1.messageId()));
                Optional.ofNullable(ack1).ifPresent(Ack::stop);
                session.write(MqttMessageBuilder.publishRelMessage(header1.messageId()), true);
                break;
            case PUBREL:
                // QoS 2 step 2
                MqttMessageIdVariableHeader header2 = (MqttMessageIdVariableHeader) message.variableHeader();
                MqttPublishMessage pmsg = session.removeQos2Msg(header2.messageId());
                session.write(MqttMessageBuilder.publishCompMessage(header2.messageId()), false);
                if (pmsg != null) {
                    TopicRegistry topicRegistry = context.getTopicRegistry();
                    MessageRegistry messageRegistry = context.getMessageRegistry();
                    Set<SubscribeTopic> subscribeTopics = topicRegistry.getSubscribesByTopic(pmsg.variableHeader().topicName(), pmsg.fixedHeader().qosLevel());
                    subscribeTopics.stream()
                            .filter(t1 -> filterOfflineSession(t1.getSession(), messageRegistry, MessageUtils.wrapPublishMessage(pmsg, t1.getQoS(), t1.getSession().generateMessageId())))
                            .forEach(t2 -> t2.getSession().write(MessageUtils.wrapPublishMessage(pmsg, t2.getQoS(), t2.getSession().generateMessageId()), t2.getQoS().value() > 0));
                    Optional.ofNullable(context.getTimeAckManager().getAck(session.generateId(MqttMessageType.PUBREC, header2.messageId()))).ifPresent(Ack::stop);
                }
                break;
            case PUBCOMP:
                // QoS 2 step 3
                MqttMessageIdVariableHeader header3 = (MqttMessageIdVariableHeader) message.variableHeader();
                Ack ack3 = context.getTimeAckManager().getAck(session.generateId(MqttMessageType.PUBREL, header3.messageId()));
                Optional.ofNullable(ack3).ifPresent(Ack::stop);
                break;
            case PINGRESP:
            default:
                break;
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
