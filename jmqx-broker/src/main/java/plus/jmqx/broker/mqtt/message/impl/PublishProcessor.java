package plus.jmqx.broker.mqtt.message.impl;

import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttVersion;
import lombok.extern.slf4j.Slf4j;
import plus.jmqx.broker.acl.AclAction;
import plus.jmqx.broker.acl.AclManager;
import plus.jmqx.broker.mqtt.channel.MqttSession;
import plus.jmqx.broker.mqtt.channel.SessionStatus;
import plus.jmqx.broker.mqtt.context.ReceiveContext;
import plus.jmqx.broker.mqtt.message.*;
import plus.jmqx.broker.mqtt.message.dispatch.PublishMessage;
import plus.jmqx.broker.mqtt.registry.MessageRegistry;
import plus.jmqx.broker.mqtt.registry.TopicRegistry;
import plus.jmqx.broker.mqtt.registry.impl.Event;
import plus.jmqx.broker.mqtt.topic.SubscribeTopic;
import plus.jmqx.broker.mqtt.util.MessageUtils;
import reactor.util.context.ContextView;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_NOT_AUTHORIZED_5;

/**
 * PUBLISH 消息流程处理
 *
 * @author maxid
 * @since 2025/4/9 16:30
 */
@Slf4j
public class PublishProcessor extends NamespceMessageProcessor<MqttPublishMessage> {

    private static final List<MqttMessageType> MESSAGE_TYPES = new ArrayList<>();

    static {
        MESSAGE_TYPES.add(MqttMessageType.PUBLISH);
    }

    /**
     * 返回处理的消息类型列表
     *
     * @return 消息类型列表
     */
    @Override
    public List<MqttMessageType> getMqttMessageTypes() {
        return MESSAGE_TYPES;
    }

    /**
     * 返回发布消息类型包装
     *
     * @return 发布消息类型包装类
     */
    @Override
    public Class<PublishMessageType> getMessageType() {
        return PublishMessageType.class;
    }

    /**
     * 处理发布消息主流程
     *
     * @param wrapper 消息包装
     * @param session 会话
     * @param view    上下文视图
     */
    @Override
    public void process(MessageWrapper<MqttPublishMessage> wrapper, MqttSession session, ContextView view) {
        ReceiveContext<?> context = view.get(ReceiveContext.class);
        try {
            // MetricManagerHolder.metricManager.getMetricRegistry().getMetricCounter(CounterType.PUBLISH_EVENT).increment();
            MqttPublishMessage message = wrapper.getMessage();
            MqttPublishVariableHeader header = message.variableHeader();
            AclManager aclManager = context.getAclManager();
            if (!session.getIsCluster() && !aclManager.check(session, header.topicName(), AclAction.PUBLISH)) {
                sendRejectAck(session, message.fixedHeader().qosLevel(), header.packetId());
                log.debug("mqtt【{}】publish topic 【{}】 acl not authorized ", session.getConnection(), header.topicName());
                return;
            }

            TopicRegistry topicRegistry = context.getTopicRegistry();
            MessageRegistry messageRegistry = context.getMessageRegistry();
            Set<SubscribeTopic> topics = topicRegistry.getSubscribesByTopic(header.topicName(), message.fixedHeader().qosLevel());
            // 分发设备上报消息
            String topicName = header.topicName();
            if (!wrapper.getClustered() && !Event.CONNECT.topicName().equals(topicName) && !Event.CLOSE.topicName().equals(topicName)) {
                context.dispatch(d -> d.onPublish(PublishMessage.builder()
                                .clientId(session.getClientId())
                                .username(session.getUsername())
                                .topic(header.topicName())
                                .payload(MessageUtils.copyReleaseByteBuf(message.payload()))
                                .build())
                        .subscribeOn(contextHolder().getDispatchScheduler())
                        .subscribe());
            }
            // 缓存 Retain 消息
            if (message.fixedHeader().isRetain()) {
                messageRegistry.saveRetainMessage(RetainMessage.of(message));
            }
            // 集群节点消息广播
            if (session.getIsCluster()) {
                send(topics, message, messageRegistry);
                return;
            }
            // MQTT QoS 处理
            MqttQoS qos = message.fixedHeader().qosLevel();
            switch (qos) {
                case AT_LEAST_ONCE:
                    session.write(MqttMessageBuilder.publishAckMessage(header.packetId()), false);
                    break;
                case EXACTLY_ONCE:
                    session.cacheQos2Msg(header.packetId(), MessageUtils.wrapPublishMessage(message, qos, 0));
                    session.write(MqttMessageBuilder.publishRecMessage(header.packetId()), false);
                    return;
                default:
                    break;
            }
            send(topics, message, messageRegistry);
        } catch (Exception e) {
            log.error("error ", e);
        }
    }

    /**
     * MQTT V5 时发送拒绝确认消息
     * @param session 会话
     * @param qos QoS
     * @param packetId 消息ID
     */
    private void sendRejectAck(MqttSession session, MqttQoS qos, int packetId) {
        if (session.getProtocolVersion() != MqttVersion.MQTT_5.protocolLevel()) {
            return;
        }
        byte reasonCode = CONNECTION_REFUSED_NOT_AUTHORIZED_5.byteValue();
        switch (qos) {
            case AT_LEAST_ONCE:
                session.write(MqttMessageBuilder.publishAckMessage(packetId, reasonCode), false);
                break;
            case EXACTLY_ONCE:
                session.write(MqttMessageBuilder.publishRecMessage(packetId, reasonCode), false);
                break;
            default:
                break;
        }
    }

    /**
     * 将消息发送给匹配的订阅者
     *
     * @param subscribeTopics 订阅集合
     * @param message         发布消息
     * @param messageRegistry 消息注册中心
     *
     */
    private void send(Set<SubscribeTopic> subscribeTopics, MqttPublishMessage message, MessageRegistry messageRegistry) {
        subscribeTopics.stream()
                .filter(t1 -> filterOfflineSession(t1.getSession(), messageRegistry, message))
                .forEach(t2 -> {
                    MqttPublishMessage pmsg = MessageUtils.wrapPublishMessage(message, t2.getQoS(), t2.getSession().generateMessageId());
                    t2.getSession().write(pmsg, t2.getQoS().value() > 0);
                });
    }

    /**
     * 离线会话缓存消息并跳过发送
     *
     * @param session         会话
     * @param messageRegistry 消息注册中心
     * @param message         发布消息
     * @return 是否可发送
     *
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
