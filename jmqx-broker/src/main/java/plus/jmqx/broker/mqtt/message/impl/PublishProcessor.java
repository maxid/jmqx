package plus.jmqx.broker.mqtt.message.impl;

import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import lombok.extern.slf4j.Slf4j;
import plus.jmqx.broker.acl.AclAction;
import plus.jmqx.broker.acl.AclManager;
import plus.jmqx.broker.mqtt.channel.SessionStatus;
import plus.jmqx.broker.mqtt.channel.MqttSession;
import plus.jmqx.broker.mqtt.context.ContextHolder;
import plus.jmqx.broker.mqtt.context.ReceiveContext;
import plus.jmqx.broker.mqtt.message.*;
import plus.jmqx.broker.mqtt.message.dispatch.PublishMessage;
import plus.jmqx.broker.mqtt.registry.MessageRegistry;
import plus.jmqx.broker.mqtt.registry.TopicRegistry;
import plus.jmqx.broker.mqtt.registry.impl.Event;
import plus.jmqx.broker.mqtt.topic.SubscribeTopic;
import plus.jmqx.broker.mqtt.util.MessageUtils;
import reactor.core.publisher.Mono;
import reactor.util.context.ContextView;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * PUBLISH 消息流程处理
 *
 * @author maxid
 * @since 2025/4/9 16:30
 */
@Slf4j
public class PublishProcessor implements MessageProcessor<MqttPublishMessage> {

    private static final List<MqttMessageType> MESSAGE_TYPES = new ArrayList<>();

    static {
        MESSAGE_TYPES.add(MqttMessageType.PUBLISH);
    }

    @Override
    public List<MqttMessageType> getMqttMessageTypes() {
        return MESSAGE_TYPES;
    }

    @Override
    public Mono<Void> process(MessageWrapper<MqttPublishMessage> wrapper, MqttSession session, ContextView view) {
        ReceiveContext<?> context = view.get(ReceiveContext.class);
        try {
            // MetricManagerHolder.metricManager.getMetricRegistry().getMetricCounter(CounterType.PUBLISH_EVENT).increment();
            MqttPublishMessage message = wrapper.getMessage();
            MqttPublishVariableHeader header = message.variableHeader();
            AclManager aclManager = context.getAclManager();
            if (!session.getIsCluster() && !aclManager.check(session, header.topicName(), AclAction.PUBLISH)) {
                log.debug("mqtt【{}】publish topic 【{}】 acl not authorized ", session.getConnection(), header.topicName());
                return Mono.empty();
            }

            TopicRegistry topicRegistry = context.getTopicRegistry();
            MessageRegistry messageRegistry = context.getMessageRegistry();
            Set<SubscribeTopic> topics = topicRegistry.getSubscribesByTopic(header.topicName(), message.fixedHeader().qosLevel());
            // 分发设备上报消息
            String topicName = header.topicName();
            if(!Event.CONNECT.topicName().equals(topicName) && !Event.CLOSE.topicName().equals(topicName)) {
                context.dispatch(d -> d.onPublish(PublishMessage.builder()
                                .clientId(session.getClientId())
                                .username(session.getUsername())
                                .topic(header.topicName())
                                .payload(MessageUtils.copyReleaseByteBuf(message.payload()))
                                .build())
                        .subscribeOn(ContextHolder.getDispatchScheduler())
                        .subscribe());
            }
            // 缓存 Retain 消息
            if (message.fixedHeader().isRetain()) {
                messageRegistry.saveRetainMessage(RetainMessage.of(message));
            }
            // 集群节点消息广播
            if (session.getIsCluster()) {
                return send(topics, message, messageRegistry);
            }
            // MQTT QoS 处理
            MqttQoS qos = message.fixedHeader().qosLevel();
            switch (qos) {
                case AT_MOST_ONCE:
                    return send(topics, message, messageRegistry);
                case AT_LEAST_ONCE:
                    return session.write(MqttMessageBuilder.publishAckMessage(header.packetId()), false)
                            .then(send(topics, message, messageRegistry));
                case EXACTLY_ONCE:
                    if (!session.existQos2Msg(header.packetId())) {
                        return session.cacheQos2Msg(header.packetId(),
                                        MessageUtils.wrapPublishMessage(message, qos, 0))
                                .then(session.write(MqttMessageBuilder.publishRecMessage(header.packetId()), false));
                    }
                default:
                    return Mono.empty();
            }
        } catch (Exception e) {
            log.error("error ", e);
        }
        return Mono.empty();
    }

    /**
     * 通用发送消息
     *
     * @param subscribeTopics {@link SubscribeTopic}
     * @param message         {@link MqttPublishMessage}
     * @param messageRegistry {@link MessageRegistry}
     * @return Mono
     */
    private Mono<Void> send(Set<SubscribeTopic> subscribeTopics, MqttPublishMessage message, MessageRegistry messageRegistry) {
        return Mono.when(subscribeTopics.stream()
                        .filter(t1 -> filterOfflineSession(t1.getSession(), messageRegistry, message))
                        .map(t2 -> t2.getSession().write(MessageUtils.wrapPublishMessage(
                                message, t2.getQoS(), t2.getSession().generateMessageId()
                        ), t2.getQoS().value() > 0))
                        .collect(Collectors.toList()));
    }

    /**
     * 过滤离线会话消息
     *
     * @param session         {@link MqttSession}
     * @param messageRegistry {@link MessageRegistry}
     * @param message         {@link MqttPublishMessage}
     * @return boolean
     */
    private boolean filterOfflineSession(MqttSession session, MessageRegistry messageRegistry, MqttPublishMessage message) {
        if (session.getStatus() == SessionStatus.ONLINE) {
            return true;
        } else {
            messageRegistry.saveSessionMessage(SessionMessage.of(session.getClientId(), message));
            return false;
        }
    }
}
