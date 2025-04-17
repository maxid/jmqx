package plus.jmqx.broker.mqtt.topic.impl;

import io.netty.handler.codec.mqtt.MqttQoS;
import plus.jmqx.broker.mqtt.channel.MqttChannel;
import plus.jmqx.broker.mqtt.topic.SubscribeTopic;
import plus.jmqx.broker.mqtt.topic.TopicFilter;
import plus.jmqx.broker.mqtt.topic.TreeNode;

import java.util.Set;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

/**
 * 树结构过滤器
 *
 * @author maxid
 * @since 2025/4/16 10:17
 */
public class TreeTopicFilter implements TopicFilter {
    private final TreeNode rootTreeNode = new TreeNode("root");

    private final LongAdder subscribeNumber = new LongAdder();

    @Override
    public Set<SubscribeTopic> getSubscribeByTopic(String topic, MqttQoS mqttQoS) {
        return rootTreeNode.getSubscribeByTopic(topic).stream().map(tp -> tp.compareQos(mqttQoS)).collect(Collectors.toSet());
    }

    @Override
    public void addSubscribeTopic(String topicFilter, MqttChannel mqttChannel, MqttQoS mqttQoS) {
        this.addSubscribeTopic(new SubscribeTopic(topicFilter, mqttQoS, mqttChannel));
    }

    @Override
    public void addSubscribeTopic(SubscribeTopic subscribeTopic) {
        if (rootTreeNode.addSubscribeTopic(subscribeTopic)) {
            subscribeNumber.add(1);
            //MetricManagerHolder.metricManager.getMetricRegistry().getMetricCounter(CounterType.SUBSCRIBE).increment();
            subscribeTopic.linkSubscribe();
        }
    }

    @Override
    public void removeSubscribeTopic(SubscribeTopic subscribeTopic) {
        if (rootTreeNode.removeSubscribeTopic(subscribeTopic)) {
            subscribeNumber.add(-1);
            //MetricManagerHolder.metricManager.getMetricRegistry().getMetricCounter(CounterType.SUBSCRIBE).decrement();
            subscribeTopic.unLinkSubscribe();
        }
    }

    @Override
    public int count() {
        return (int) subscribeNumber.sum();
    }

    @Override
    public Set<SubscribeTopic> getAllSubscribesTopic() {
        return rootTreeNode.getAllSubscribesTopic();
    }
}
