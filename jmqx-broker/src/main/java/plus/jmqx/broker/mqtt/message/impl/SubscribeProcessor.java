package plus.jmqx.broker.mqtt.message.impl;

import cn.hutool.core.collection.CollUtil;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import io.netty.handler.codec.mqtt.MqttVersion;
import lombok.extern.slf4j.Slf4j;
import plus.jmqx.broker.acl.AclAction;
import plus.jmqx.broker.acl.AclManager;
import plus.jmqx.broker.cluster.ClusterMessage;
import plus.jmqx.broker.cluster.ClusterRegistry;
import plus.jmqx.broker.metrics.MetricsManagerHolder;
import plus.jmqx.broker.mqtt.MqttConfiguration;
import plus.jmqx.broker.mqtt.channel.MqttSession;
import plus.jmqx.broker.mqtt.context.ReceiveContext;
import plus.jmqx.broker.mqtt.message.MessageWrapper;
import plus.jmqx.broker.mqtt.message.MqttMessageBuilder;
import plus.jmqx.broker.mqtt.message.NamespceMessageProcessor;
import plus.jmqx.broker.mqtt.message.SubscribeTopicMessage;
import plus.jmqx.broker.mqtt.registry.MessageRegistry;
import plus.jmqx.broker.mqtt.registry.TopicRegistry;
import plus.jmqx.broker.mqtt.topic.SubscribeTopic;
import plus.jmqx.broker.concurrent.SchedulerTasks;
import reactor.util.context.ContextView;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_NOT_AUTHORIZED_5;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_UNSPECIFIED_ERROR;

/**
 * SUBSCRIBE 消息流程处理
 *
 * @author maxid
 * @since 2025/4/9 16:31
 */
@Slf4j
public class SubscribeProcessor extends NamespceMessageProcessor<MqttSubscribeMessage> {

    private static final List<MqttMessageType> MESSAGE_TYPES = new ArrayList<>();

    static {
        MESSAGE_TYPES.add(MqttMessageType.SUBSCRIBE);
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
     * 返回订阅消息类型包装
     *
     * @return 订阅消息类型包装类
     */
    @Override
    public Class<SubscribeMessageType> getMessageType() {
        return SubscribeMessageType.class;
    }

    /**
     * 处理订阅消息并注册订阅关系
     *
     * @param wrapper 消息包装
     * @param session 会话
     * @param view    上下文视图
     */
    @Override
    public void process(MessageWrapper<MqttSubscribeMessage> wrapper, MqttSession session, ContextView view) {
        MqttSubscribeMessage message = wrapper.getMessage();
        MetricsManagerHolder.get().incrementSubscribeEvents();
        ReceiveContext<?> context = view.get(ReceiveContext.class);
        TopicRegistry topicRegistry = context.getTopicRegistry();
        MessageRegistry messageRegistry = context.getMessageRegistry();
        AclManager aclManager = context.getAclManager();
        int denyReasonCode = session.getProtocolVersion() == MqttVersion.MQTT_5.protocolLevel()
                ? CONNECTION_REFUSED_NOT_AUTHORIZED_5.byteValue() : CONNECTION_REFUSED_UNSPECIFIED_ERROR.byteValue();
        final int subscriptionLimit;
        if (context.getConfiguration() instanceof MqttConfiguration mqttCfg
                && mqttCfg.getMaxTopicSubscriptions() != null && mqttCfg.getMaxTopicSubscriptions() > 0) {
            subscriptionLimit = mqttCfg.getMaxTopicSubscriptions();
        } else {
            subscriptionLimit = 0;
        }
        // 提前拷贝订阅列表，避免异步 ACL 完成后依赖可能已释放的报文对象
        List<MqttTopicSubscription> subscriptions = new ArrayList<>(message.payload().topicSubscriptions());
        int messageId = message.variableHeader().messageId();
        int existingTopicCount = session.getTopics().size();

        context.getAclExecutor().supply(
                () -> evaluateSubscriptions(aclManager, session, subscriptions, subscriptionLimit,
                        existingTopicCount, denyReasonCode),
                emptyResult(subscriptions.size(), denyReasonCode),
                session.getClientId()
        ).whenComplete((result, ex) -> {
            Runnable apply = () -> {
                SubscribeAclResult aclResult = result == null
                        ? emptyResult(subscriptions.size(), denyReasonCode) : result;
                applySubscribeResult(context, session, topicRegistry, messageRegistry, subscriptions, messageId, aclResult);
            };
            // 非阻塞 ACL：supply 已在当前（control）线程完成，直接应用，避免再 schedule 一次
            if (!context.getAclExecutor().requiresOffload()) {
                apply.run();
            } else {
                scheduleOnControl(apply);
            }
        });
    }

    private SubscribeAclResult evaluateSubscriptions(AclManager aclManager,
                                                     MqttSession session,
                                                     List<MqttTopicSubscription> subscriptions,
                                                     int subscriptionLimit,
                                                     int existingTopicCount,
                                                     int denyReasonCode) {
        Set<SubscribeTopic> topics = new LinkedHashSet<>();
        List<Integer> reasonCodes = new ArrayList<>(subscriptions.size());
        int addedCount = 0;
        for (MqttTopicSubscription subscription : subscriptions) {
            if (subscriptionLimit > 0 && existingTopicCount + addedCount >= subscriptionLimit) {
                log.warn("max subscriptions ({}) reached for [{}]", subscriptionLimit, session.getClientId());
                reasonCodes.add(denyReasonCode);
                continue;
            }
            SubscribeTopic topic = new SubscribeTopic(subscription.topicFilter(), subscription.qualityOfService(), session);
            if (aclManager.check(session, topic.getTopicFilter(), AclAction.SUBSCRIBE)) {
                topics.add(topic);
                addedCount++;
                reasonCodes.add(subscription.qualityOfService().value());
            } else {
                reasonCodes.add(denyReasonCode);
            }
        }
        return new SubscribeAclResult(topics, reasonCodes);
    }

    private void applySubscribeResult(ReceiveContext<?> context,
                                      MqttSession session,
                                      TopicRegistry topicRegistry,
                                      MessageRegistry messageRegistry,
                                      List<MqttTopicSubscription> subscriptions,
                                      int messageId,
                                      SubscribeAclResult result) {
        Set<SubscribeTopic> topics = result.topics();
        if (CollUtil.isNotEmpty(topics)) {
            topicRegistry.registrySubscribesTopic(topics);
            clusterSubscribe(context, topics);
            for (int i = 0; i < subscriptions.size(); i++) {
                if (i < result.reasonCodes().size()
                        && result.reasonCodes().get(i) == subscriptions.get(i).qualityOfService().value()) {
                    loadRetainMessage(messageRegistry, session, subscriptions.get(i));
                }
            }
        }
        session.write(MqttMessageBuilder.subAckMessage(messageId, result.reasonCodes()), false);
    }

    private static SubscribeAclResult emptyResult(int size, int denyReasonCode) {
        List<Integer> reasonCodes = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            reasonCodes.add(denyReasonCode);
        }
        return new SubscribeAclResult(Set.of(), reasonCodes);
    }

    private void clusterSubscribe(ReceiveContext<?> context, Set<SubscribeTopic> topics) {
        ClusterRegistry registry = context.getClusterRegistry();
        if (registry == null) {
            return;
        }
        MqttConfiguration.ClusterConfig config = context.getConfiguration().getClusterConfig();
        if (config == null || !config.isEnabled()) {
            return;
        }
        String nodeId = config.getClusterId();
        for (SubscribeTopic topic : topics) {
            SubscribeTopicMessage stm = new SubscribeTopicMessage(nodeId, topic.getTopicFilter(), true);
            SchedulerTasks.subscribeOnCluster(contextHolder(),
                            registry.spreadPublishMessage(new ClusterMessage(stm, ClusterMessage.ClusterEvent.SUBSCRIBE)))
                    .subscribe();
        }
    }

    /**
     * 下发匹配的保留消息
     *
     * @param messageRegistry 消息注册中心
     * @param session         会话
     * @param subscription    订阅信息
     */
    private void loadRetainMessage(MessageRegistry messageRegistry, MqttSession session, MqttTopicSubscription subscription) {
        int topicQos = subscription.qualityOfService().value();
        String topic = subscription.topicFilter();
        messageRegistry.getRetainMessage(topic).forEach(msg ->
                session.write(msg.toPublishMessage(session, topicQos), Math.min(topicQos, msg.getQos()) > 0));
    }

    private record SubscribeAclResult(Set<SubscribeTopic> topics, List<Integer> reasonCodes) {
    }

}
