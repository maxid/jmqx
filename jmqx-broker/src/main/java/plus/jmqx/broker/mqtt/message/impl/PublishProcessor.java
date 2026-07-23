package plus.jmqx.broker.mqtt.message.impl;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttVersion;
import lombok.extern.slf4j.Slf4j;
import plus.jmqx.broker.acl.AclAction;
import plus.jmqx.broker.metrics.MetricsManagerHolder;
import plus.jmqx.broker.mqtt.MqttConfiguration;
import plus.jmqx.broker.mqtt.channel.MqttSession;
import plus.jmqx.broker.mqtt.channel.SessionStatus;
import plus.jmqx.broker.mqtt.context.ReceiveContext;
import plus.jmqx.broker.mqtt.message.MessageWrapper;
import plus.jmqx.broker.mqtt.message.MqttMessageBuilder;
import plus.jmqx.broker.mqtt.message.NamespceMessageProcessor;
import plus.jmqx.broker.mqtt.message.RetainMessage;
import plus.jmqx.broker.mqtt.message.SessionMessage;
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
            MetricsManagerHolder.get().incrementPublishedMessages();
            MqttPublishMessage message = wrapper.getMessage();
            MqttPublishVariableHeader header = message.variableHeader();

            // 集群节点转发消息跳过 ACL；设备侧 ACL 卸载到独立线程池，避免阻塞 jmqx-publish-io
            if (!session.getIsCluster()) {
                Object payload = message.payload();
                if (payload instanceof ByteBuf buf) {
                    // 对抗 process 返回后 channel/dispatcher 释放 payload，保证 ACL 回调时仍可读
                    buf.retain();
                }
                context.getAclExecutor().check(session, header.topicName(), AclAction.PUBLISH)
                        .whenComplete((passed, ex) -> {
                            try {
                                if (!Boolean.TRUE.equals(passed)) {
                                    sendRejectAck(session, message.fixedHeader().qosLevel(), header.packetId());
                                    log.debug("mqtt【{}】publish topic 【{}】 acl not authorized ",
                                            session.getConnection(), header.topicName());
                                    return;
                                }
                                processAuthorized(wrapper, session, context, message, header);
                            } catch (Exception e) {
                                log.error("error ", e);
                            } finally {
                                if (payload instanceof ByteBuf buf) {
                                    buf.release();
                                }
                            }
                        });
                return;
            }

            processAuthorized(wrapper, session, context, message, header);
        } catch (Exception e) {
            log.error("error ", e);
        }
    }

    /**
     * ACL 通过后的发布处理
     *
     * @param wrapper 消息包装
     * @param session 会话
     * @param context 接收上下文
     * @param message 发布消息
     * @param header  可变头
     */
    private void processAuthorized(MessageWrapper<MqttPublishMessage> wrapper,
                                   MqttSession session,
                                   ReceiveContext<?> context,
                                   MqttPublishMessage message,
                                   MqttPublishVariableHeader header) {
        // === 定向投递分支：平台向指定 clientId 设备下发消息 ===
        String clientId = wrapper.getClientId();
        if (clientId != null) {
            send(clientId, message, context);
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
                if (!session.cacheQos2Msg(header.packetId(), MessageUtils.wrapPublishMessage(message, qos, 0))) {
                    return;
                }
                session.write(MqttMessageBuilder.publishRecMessage(header.packetId()), false);
                return;
            default:
                break;
        }
        send(topics, message, messageRegistry);
    }

    /**
     * 定向投递：目标设备须已订阅该主题，才写入 Session（符合 MQTT 订阅语义）
     *
     * @param clientId 目标设备 clientId
     * @param message  MQTT 发布消息
     * @param context  接收上下文
     */
    private void send(String clientId, MqttPublishMessage message, ReceiveContext<?> context) {
        MqttSession session = context.getSessionRegistry().get(clientId);
        if (session == null || !session.active()) {
            MqttConfiguration.ClusterConfig config = context.getConfiguration().getClusterConfig();
            log.debug("[{}] publish: device [{}] not online, skip", config.getClusterId(), clientId);
            return;
        }
        String topicName = message.variableHeader().topicName();
        MqttQoS publishQos = message.fixedHeader().qosLevel();
        // 按 MQTT 规范：未订阅目标主题则不下发（定向投递不能绕过订阅关系）
        SubscribeTopic matched = context.getTopicRegistry()
                .getSubscribesByTopic(topicName, publishQos)
                .stream()
                .filter(t -> clientId.equals(t.getSession().getClientId()))
                .findFirst()
                .orElse(null);
        if (matched == null) {
            log.debug("skip publish to [{}]: not subscribed to [{}]", clientId, topicName);
            return;
        }
        int packetId = session.generateMessageId();
        if (packetId < 0) {
            log.warn("skip publish to [{}]: no available packet ID", clientId);
            return;
        }
        MqttPublishMessage pmsg = MessageUtils.wrapPublishMessage(message, matched.getQoS(), packetId);
        session.write(pmsg, matched.getQoS().value() > 0);
    }

    /**
     * MQTT V5 时发送拒绝确认消息
     *
     * @param session  会话
     * @param qos      QoS
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
     */
    private void send(Set<SubscribeTopic> subscribeTopics, MqttPublishMessage message, MessageRegistry messageRegistry) {
        subscribeTopics.stream()
                .filter(t1 -> filterOfflineSession(t1.getSession(), messageRegistry, message))
                .forEach(t2 -> {
                    int packetId = t2.getSession().generateMessageId();
                    if (packetId < 0) {
                        log.warn("skip publish to [{}]: no available packet ID", t2.getSession().getClientId());
                        return;
                    }
                    MqttPublishMessage pmsg = MessageUtils.wrapPublishMessage(message, t2.getQoS(), packetId);
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
