package plus.jmqx.broker.mqtt.message.impl;

import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;
import plus.jmqx.broker.cluster.ClusterMessage;
import plus.jmqx.broker.cluster.ClusterRegistry;
import plus.jmqx.broker.mqtt.MqttConfiguration;
import plus.jmqx.broker.mqtt.channel.MqttSession;
import plus.jmqx.broker.mqtt.context.ReceiveContext;
import plus.jmqx.broker.mqtt.message.MessageWrapper;
import plus.jmqx.broker.mqtt.message.MqttMessageBuilder;
import plus.jmqx.broker.mqtt.message.NamespceMessageProcessor;
import plus.jmqx.broker.mqtt.message.SubscribeTopicMessage;
import plus.jmqx.broker.mqtt.registry.TopicRegistry;
import plus.jmqx.broker.mqtt.topic.SubscribeTopic;
import reactor.core.scheduler.Schedulers;
import reactor.util.context.ContextView;

import java.util.ArrayList;
import java.util.List;

/**
 * UNSUBSCRIBE 消息流程处理
 *
 * @author maxid
 * @since 2025/4/9 16:33
 */
public class UnsubscribeProcessor extends NamespceMessageProcessor<MqttUnsubscribeMessage> {

    private static final List<MqttMessageType> MESSAGE_TYPES = new ArrayList<>();

    static {
        MESSAGE_TYPES.add(MqttMessageType.UNSUBSCRIBE);
    }

    @Override
    public List<MqttMessageType> getMqttMessageTypes() {
        return MESSAGE_TYPES;
    }

    @Override
    public Class<UnsubscribeMessageType> getMessageType() {
        return UnsubscribeMessageType.class;
    }

    @Override
    public void process(MessageWrapper<MqttUnsubscribeMessage> wrapper, MqttSession session, ContextView view) {
        MqttUnsubscribeMessage msg = wrapper.getMessage();
        ReceiveContext<?> context = view.get(ReceiveContext.class);
        TopicRegistry topicRegistry = context.getTopicRegistry();
        msg.payload()
                .topics()
                .stream()
                .map(topic -> new SubscribeTopic(topic, MqttQoS.AT_MOST_ONCE, session))
                .forEach(topic -> {
                    topicRegistry.removeSubscribeTopic(topic);
                    clusterUnsubscribe(context, topic.getTopicFilter());
                });
        session.write(MqttMessageBuilder.unsubAckMessage(msg.variableHeader().messageId()), false);
    }

    private void clusterUnsubscribe(ReceiveContext<?> context, String topicFilter) {
        ClusterRegistry registry = context.getClusterRegistry();
        if (registry == null) return;
        MqttConfiguration.ClusterConfig config = context.getConfiguration().getClusterConfig();
        if (config == null || !config.isEnabled()) return;
        SubscribeTopicMessage stm = new SubscribeTopicMessage(config.getClusterId(), topicFilter, false);
        registry.spreadPublishMessage(new ClusterMessage(stm, ClusterMessage.ClusterEvent.SUBSCRIBE))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe();
    }

}
