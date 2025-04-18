package plus.jmqx.broker.mqtt.message.impl;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.*;
import lombok.extern.slf4j.Slf4j;
import plus.jmqx.broker.auth.AuthManager;
import plus.jmqx.broker.cluster.ClusterMessage;
import plus.jmqx.broker.config.ConnectMode;
import plus.jmqx.broker.mqtt.channel.SessionStatus;
import plus.jmqx.broker.mqtt.channel.MqttSession;
import plus.jmqx.broker.mqtt.context.MqttReceiveContext;
import plus.jmqx.broker.mqtt.context.ReceiveContext;
import plus.jmqx.broker.mqtt.message.CloseMqttMessage;
import plus.jmqx.broker.mqtt.message.MessageProcessor;
import plus.jmqx.broker.mqtt.message.MessageWrapper;
import plus.jmqx.broker.mqtt.message.MqttMessageBuilder;
import plus.jmqx.broker.mqtt.registry.ChannelRegistry;
import plus.jmqx.broker.mqtt.registry.EventRegistry;
import plus.jmqx.broker.mqtt.registry.MessageRegistry;
import plus.jmqx.broker.mqtt.registry.TopicRegistry;
import plus.jmqx.broker.mqtt.registry.impl.Event;
import plus.jmqx.broker.mqtt.topic.SubscribeTopic;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.context.ContextView;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * CONNECT 消息流程处理
 *
 * @author maxid
 * @since 2025/4/9 16:29
 */
@Slf4j
public class ConnectProcessor implements MessageProcessor<MqttConnectMessage> {

    private static final List<MqttMessageType> MESSAGE_TYPES = new ArrayList<>();

    private static final int MILLI_SECOND_PERIOD = 1_000;

    static {
        MESSAGE_TYPES.add(MqttMessageType.CONNECT);
    }

    @Override
    public List<MqttMessageType> getMqttMessageTypes() {
        return MESSAGE_TYPES;
    }

    @Override
    public Mono<Void> process(MessageWrapper<MqttConnectMessage> wrapper, MqttSession session, ContextView view) {
        MqttConnectMessage message = wrapper.getMessage();
        MqttReceiveContext context = (MqttReceiveContext) view.get(ReceiveContext.class);
        MqttConnectVariableHeader header = message.variableHeader();
        MqttConnectPayload payload = message.payload();
        String clientId = payload.clientIdentifier();
        String username = payload.userName();
        byte[] password = payload.passwordInBytes();
        ChannelRegistry channelRegistry = context.getChannelRegistry();
        TopicRegistry topicRegistry = context.getTopicRegistry();
        EventRegistry eventRegistry = context.getEventRegistry();
        byte mqttVersion = (byte) header.version();
        AuthManager authManager = context.getAuthManager();
        // 处理同一个设备多个连接的情况
        MqttSession clientSession = channelRegistry.get(clientId);
        if (context.getConfiguration().getConnectMode() == ConnectMode.UNIQUE) {
            if (clientSession != null && clientSession.getStatus() == SessionStatus.ONLINE) {
                return rejected(session, mqttVersion);
            }
        } else {
            if (clientSession != null && clientSession.getStatus() == SessionStatus.ONLINE) {
                if (System.currentTimeMillis() - clientSession.getConnectTime() > (context.getConfiguration().getNotKickSeconds() * 1000)) {
                    clientSession.close().subscribe();
                } else {
                    return rejected(session, mqttVersion);
                }
            }
        }
        // 协议版本支持检验
        if (MqttVersion.MQTT_3_1.protocolLevel() != mqttVersion &&
                MqttVersion.MQTT_3_1_1.protocolLevel() != mqttVersion &&
                MqttVersion.MQTT_5.protocolLevel() != mqttVersion) {
            return badVersion(session, mqttVersion);
        }
        // 鉴权认证
        if (!authManager.auth(clientId, username, password)) {
            return badCredentials(session, mqttVersion);
        }
        // START 处理连接业务：建立连接会话等
        session.disposableClose(); // cancel defer close not authenticate channel
        // 会话遗愿消息初始化
        if (header.isWillFlag()) {
            session.setWill(MqttSession.Will.builder()
                    .isRetain(header.isWillRetain())
                    .willTopic(payload.willTopic())
                    .willMessage(payload.willMessageInBytes())
                    .mqttQoS(MqttQoS.valueOf(header.willQos()))
                    .build());
        }
        // 初始化会话信息
        session.setClientId(clientId);
        session.setAuthTime(System.currentTimeMillis());
        session.setConnectTime(System.currentTimeMillis());
        session.setKeepalive(header.keepAliveTimeSeconds());
        session.setSessionPersistent(!header.isCleanSession());
        session.setStatus(SessionStatus.ONLINE);
        session.setUsername(username);
        // 设置读闲置处理
        long idleTimeout = (long) header.keepAliveTimeSeconds() * MILLI_SECOND_PERIOD << 1;
        session.getConnection().onReadIdle(idleTimeout, () -> this.close(session, context, eventRegistry));
        // 发送集群消息，通知其它集群节点有相同的客户端ID断开连接
        CloseMqttMessage closeMqttMessage = new CloseMqttMessage();
        closeMqttMessage.setClientId(clientId);
        ClusterMessage clusterMessage = new ClusterMessage(closeMqttMessage);
        context.getClusterRegistry().spreadPublishMessage(clusterMessage).subscribe();
        // 遗愿消息处理
        session.registryClose(s1 -> Optional.ofNullable(s1.getWill())
                .ifPresent(will -> topicRegistry.getSubscribesByTopic(will.getWillTopic(), will.getMqttQoS())
                        .forEach(topic -> {
                            MqttSession s2 = topic.getMqttChannel();
                            s2.write(MqttMessageBuilder.publishMessage(
                                            false,
                                            topic.getQoS(),
                                            topic.getQoS() == MqttQoS.AT_MOST_ONCE ? 0 : s2.generateMessageId(),
                                            will.getWillTopic(),
                                            Unpooled.wrappedBuffer(will.getWillMessage())
                                    ), topic.getQoS().value() > 0)
                                    .subscribe();
                        })));
        // 各注册中心关联会话处理
        registry(session, channelRegistry, topicRegistry);
        // 注册关闭 MQTT 会话事件
        session.registryClose(s1 -> this.close(session, context, eventRegistry));
        // metricManager.getMetricRegistry().getMetricCounter(CounterType.CONNECT).increment();
        // session.registryClose(channel -> metricManager.getMetricRegistry().getMetricCounter(CounterType.CONNECT).decrement());
        // 触发连接事件
        eventRegistry.registry(Event.CONNECT, session, message, context);
        // 连接确认
        return ok(session, context, mqttVersion);
    }

    /**
     * 拒绝连接确认消息
     *
     * @param session     会话
     * @param mqttVersion 协议版本
     * @return {@link Mono}
     */
    private Mono<Void> rejected(MqttSession session, byte mqttVersion) {
        MqttConnAckMessage ack = MqttMessageBuilder.connectAckMessage(MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED, mqttVersion);
        return session.write(ack, false).then(session.close());
    }

    /**
     * 不支持协议版本确认消息
     *
     * @param session     会话
     * @param mqttVersion 协议版本
     * @return {@link Mono}
     */
    private Mono<Void> badVersion(MqttSession session, byte mqttVersion) {
        MqttConnAckMessage ack = MqttMessageBuilder.connectAckMessage(MqttConnectReturnCode.CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION, mqttVersion);
        return session.write(ack, false).then(session.close());
    }

    /**
     * 无效凭证确认消息
     *
     * @param session     会话
     * @param mqttVersion 协议版本
     * @return {@link Mono}
     */
    private Mono<Void> badCredentials(MqttSession session, byte mqttVersion) {
        MqttConnAckMessage ack = MqttMessageBuilder.connectAckMessage(MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD, mqttVersion);
        return session.write(ack, false).then(session.close());
    }

    /**
     * 连接确认消息
     *
     * @param session     会话
     * @param context     上下文
     * @param mqttVersion 协议版本
     * @return {@link Mono}
     */
    private Mono<Void> ok(MqttSession session, MqttReceiveContext context, byte mqttVersion) {
        MqttConnAckMessage ack = MqttMessageBuilder.connectAckMessage(MqttConnectReturnCode.CONNECTION_ACCEPTED, mqttVersion);
        return session.write(ack, false)
                .then(Mono.fromRunnable(() -> sendOfflineMessage(context.getMessageRegistry(), session)));
    }

    /**
     * 注册中心会话关联
     *
     * @param session         会话
     * @param channelRegistry 会话注册中心
     * @param topicRegistry   主题注册中心
     */
    private void registry(MqttSession session, ChannelRegistry channelRegistry, TopicRegistry topicRegistry) {
        Optional.ofNullable(channelRegistry.get(session.getClientId()))
                .ifPresent(s1 -> {
                    // 主题会话新关联
                    Set<SubscribeTopic> topics = s1.getTopics().stream().map(topic ->
                                    new SubscribeTopic(topic.getTopicFilter(), topic.getQoS(), session))
                            .collect(Collectors.toSet());
                    topicRegistry.registrySubscribesTopic(topics);
                    // 移除旧会话
                    channelRegistry.close(s1);
                    topicRegistry.clear(s1);
                });
        // 注册新会话
        channelRegistry.registry(session.getClientId(), session);
    }

    /**
     * 关闭会话
     *
     * @param session 会话
     * @param context 上下文
     */
    private void close(MqttSession session, MqttReceiveContext context, EventRegistry eventRegistry) {
        log.debug("【{}】【{}】【{}】", Thread.currentThread().getName(), "CLOSE", session);
        session.setStatus(SessionStatus.OFFLINE);
        if (!session.isSessionPersistent()) {
            context.getTopicRegistry().clear(session);
            context.getChannelRegistry().close(session);
        }
        eventRegistry.registry(Event.CLOSE, session, null, context);
        //metricManager.getMetricRegistry().getMetricCounter(CounterType.CLOSE_EVENT).increment();
        session.close().subscribe();
    }

    /**
     * 发送离线信息
     *
     * @param messageRegistry 消息注册中心
     * @param session         会话
     */
    private void sendOfflineMessage(MessageRegistry messageRegistry, MqttSession session) {
        Optional.ofNullable(messageRegistry.getSessionMessage(session.getClientId()))
                .ifPresent(messages -> messages.forEach(message -> {
                    session.write(message.toPublishMessage(session), message.getQos() > 0)
                            .subscribeOn(Schedulers.single())
                            .subscribe();
                }));
    }


}
