package plus.jmqx.broker.mqtt.message.impl;

import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;
import plus.jmqx.broker.mqtt.channel.MqttSession;
import plus.jmqx.broker.mqtt.context.ReceiveContext;
import plus.jmqx.broker.mqtt.message.MessageWrapper;
import plus.jmqx.broker.mqtt.message.MqttMessageBuilder;
import plus.jmqx.broker.mqtt.message.NamespceMessageProcessor;
import plus.jmqx.broker.mqtt.registry.TopicRegistry;
import plus.jmqx.broker.mqtt.topic.SubscribeTopic;
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
     * 返回去订阅消息类型包装
     *
     * @return 去订阅消息类型包装类
     */
    @Override
    public Class<UnsubscribeMessageType> getMessageType() {
        return UnsubscribeMessageType.class;
    }

    /**
     * 处理去订阅消息并移除订阅关系
     *
     * @param wrapper 消息包装
     * @param session 会话
     * @param view    上下文视图
     */
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
