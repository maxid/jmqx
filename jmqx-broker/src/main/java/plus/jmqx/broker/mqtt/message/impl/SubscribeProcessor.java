package plus.jmqx.broker.mqtt.message.impl;

import cn.hutool.core.collection.CollectionUtil;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import lombok.extern.slf4j.Slf4j;
import plus.jmqx.broker.acl.AclAction;
import plus.jmqx.broker.acl.AclManager;
import plus.jmqx.broker.mqtt.channel.MqttSession;
import plus.jmqx.broker.mqtt.context.ReceiveContext;
import plus.jmqx.broker.mqtt.message.MessageProcessor;
import plus.jmqx.broker.mqtt.message.MessageWrapper;
import plus.jmqx.broker.mqtt.message.MqttMessageBuilder;
import plus.jmqx.broker.mqtt.registry.MessageRegistry;
import plus.jmqx.broker.mqtt.registry.TopicRegistry;
import plus.jmqx.broker.mqtt.topic.SubscribeTopic;
import reactor.core.publisher.Mono;
import reactor.util.context.ContextView;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * SUBSCRIBE 消息流程处理
 *
 * @author maxid
 * @since 2025/4/9 16:31
 */
@Slf4j
public class SubscribeProcessor implements MessageProcessor<MqttSubscribeMessage> {

    private static final List<MqttMessageType> MESSAGE_TYPES = new ArrayList<>();

    static {
        MESSAGE_TYPES.add(MqttMessageType.SUBSCRIBE);
    }

    @Override
    public List<MqttMessageType> getMqttMessageTypes() {
        return MESSAGE_TYPES;
    }

    @Override
    public void process(MessageWrapper<MqttSubscribeMessage> wrapper, MqttSession session, ContextView view) {
        MqttSubscribeMessage message = wrapper.getMessage();
        // MetricManagerHolder.metricManager.getMetricRegistry().getMetricCounter(CounterType.SUBSCRIBE_EVENT).increment();
        ReceiveContext<?> context = (ReceiveContext<?>) view.get(ReceiveContext.class);
        TopicRegistry topicRegistry = context.getTopicRegistry();
        MessageRegistry messageRegistry = context.getMessageRegistry();
        AclManager aclManager = context.getAclManager();
        Set<SubscribeTopic> topics = message.payload().topicSubscriptions()
                .stream()
                .peek(s1 -> this.loadRetainMessage(messageRegistry, session, s1))
                .map(s2 -> new SubscribeTopic(s2.topicFilter(), s2.qualityOfService(), session))
                .filter(s3 -> aclManager.check(session, s3.getTopicFilter(), AclAction.SUBSCRIBE))
                .collect(Collectors.toSet());
        if (CollectionUtil.isNotEmpty(topics)) {
            topicRegistry.registrySubscribesTopic(topics);
        }
        session.write(MqttMessageBuilder.subAckMessage(
                message.variableHeader().messageId(),
                message.payload()
                        .topicSubscriptions()
                        .stream()
                        .map(s1 -> s1.qualityOfService().value())
                        .collect(Collectors.toList())
        ), false);
    }

    private void loadRetainMessage(MessageRegistry messageRegistry, MqttSession session, MqttTopicSubscription subscription) {
        int topicQos = subscription.qualityOfService().value();
        String topic = subscription.topicFilter();
        messageRegistry.getRetainMessage(topic).forEach(msg ->
                session.write(msg.toPublishMessage(session, topicQos), Math.min(topicQos, msg.getQos()) > 0));
    }
}
