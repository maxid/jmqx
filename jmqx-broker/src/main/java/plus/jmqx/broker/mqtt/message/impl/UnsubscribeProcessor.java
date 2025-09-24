package plus.jmqx.broker.mqtt.message.impl;

import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;
import plus.jmqx.broker.mqtt.channel.MqttSession;
import plus.jmqx.broker.mqtt.context.ReceiveContext;
import plus.jmqx.broker.mqtt.message.MessageProcessor;
import plus.jmqx.broker.mqtt.message.MessageWrapper;
import plus.jmqx.broker.mqtt.message.MqttMessageBuilder;
import plus.jmqx.broker.mqtt.topic.SubscribeTopic;
import plus.jmqx.broker.mqtt.registry.TopicRegistry;
import reactor.core.publisher.Mono;
import reactor.util.context.ContextView;

import java.util.ArrayList;
import java.util.List;

/**
 * UNSUBSCRIBE 消息流程处理
 *
 * @author maxid
 * @since 2025/4/9 16:33
 */
public class UnsubscribeProcessor implements MessageProcessor<MqttUnsubscribeMessage> {

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
        //MetricManagerHolder.metricManager.getMetricRegistry().getMetricCounter(CounterType.UN_SUBSCRIBE_EVENT).increment();
        ReceiveContext<?> context = view.get(ReceiveContext.class);
        TopicRegistry topicRegistry = context.getTopicRegistry();
        msg.payload()
                .topics()
                .stream()
                // 随机设置一个MqttQoS 用于删除topic订阅
                .map(topic -> new SubscribeTopic(topic, MqttQoS.AT_MOST_ONCE, session))
                .forEach(topicRegistry::removeSubscribeTopic);
        session.write(MqttMessageBuilder.unsubAckMessage(msg.variableHeader().messageId()), false);
    }
}
