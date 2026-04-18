package plus.jmqx.broker.mqtt.message.impl;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.collection.CollectionUtil;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import io.netty.handler.codec.mqtt.MqttVersion;
import lombok.extern.slf4j.Slf4j;
import plus.jmqx.broker.acl.AclAction;
import plus.jmqx.broker.acl.AclManager;
import plus.jmqx.broker.mqtt.channel.MqttSession;
import plus.jmqx.broker.mqtt.context.ReceiveContext;
import plus.jmqx.broker.mqtt.message.MessageWrapper;
import plus.jmqx.broker.mqtt.message.MqttMessageBuilder;
import plus.jmqx.broker.mqtt.message.NamespceMessageProcessor;
import plus.jmqx.broker.mqtt.registry.MessageRegistry;
import plus.jmqx.broker.mqtt.registry.TopicRegistry;
import plus.jmqx.broker.mqtt.topic.SubscribeTopic;
import reactor.util.context.ContextView;

import java.util.ArrayList;
import java.util.List;
import java.util.LinkedHashSet;
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
        // MetricManagerHolder.metricManager.getMetricRegistry().getMetricCounter(CounterType.SUBSCRIBE_EVENT).increment();
        ReceiveContext<?> context = view.get(ReceiveContext.class);
        TopicRegistry topicRegistry = context.getTopicRegistry();
        MessageRegistry messageRegistry = context.getMessageRegistry();
        AclManager aclManager = context.getAclManager();
        int reasonCode = session.getProtocolVersion() == MqttVersion.MQTT_5.protocolLevel()
                ? CONNECTION_REFUSED_NOT_AUTHORIZED_5.byteValue() : CONNECTION_REFUSED_UNSPECIFIED_ERROR.byteValue();
        Set<SubscribeTopic> topics = new LinkedHashSet<>();
        List<Integer> reasonCodes = new ArrayList<>();
        message.payload().topicSubscriptions().forEach(s -> {
            SubscribeTopic topic = new SubscribeTopic(s.topicFilter(), s.qualityOfService(), session);
            if (aclManager.check(session, topic.getTopicFilter(), AclAction.SUBSCRIBE)) {
                this.loadRetainMessage(messageRegistry, session, s);
                topics.add(topic);
                reasonCodes.add(s.qualityOfService().value());
            } else {
                reasonCodes.add(reasonCode);
            }
        });
        if (CollUtil.isNotEmpty(topics)) {
            topicRegistry.registrySubscribesTopic(topics);
        }
        session.write(MqttMessageBuilder.subAckMessage(
                message.variableHeader().messageId(),
                reasonCodes
        ), false);
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

}
