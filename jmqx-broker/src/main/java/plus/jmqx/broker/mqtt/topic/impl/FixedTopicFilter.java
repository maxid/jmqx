package plus.jmqx.broker.mqtt.topic.impl;

import io.netty.handler.codec.mqtt.MqttQoS;
import plus.jmqx.broker.mqtt.channel.MqttSession;
import plus.jmqx.broker.mqtt.topic.SubscribeTopic;
import plus.jmqx.broker.mqtt.topic.TopicFilter;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

/**
 * 尽量简洁一句描述
 *
 * @author maxid
 * @since 2025/4/16 10:16
 */
public class FixedTopicFilter implements TopicFilter {
    private final LongAdder subscribeNumber = new LongAdder();

    private final Map<String, CopyOnWriteArraySet<SubscribeTopic>> topicChannels = new ConcurrentHashMap<>();

    @Override
    public Set<SubscribeTopic> getSubscribeByTopic(String topic, MqttQoS mqttQoS) {
        CopyOnWriteArraySet<SubscribeTopic> channels = topicChannels.computeIfAbsent(topic, t -> new CopyOnWriteArraySet<>());
        return channels.stream().map(tp -> tp.compareQos(mqttQoS)).collect(Collectors.toSet());
    }

    @Override
    public void addSubscribeTopic(String topicFilter, MqttSession mqttChannel, MqttQoS mqttQoS) {
        this.addSubscribeTopic(new SubscribeTopic(topicFilter, mqttQoS, mqttChannel));
    }

    @Override
    public void addSubscribeTopic(SubscribeTopic subscribeTopic) {
        CopyOnWriteArraySet<SubscribeTopic> channels = topicChannels.computeIfAbsent(subscribeTopic.getTopicFilter(), t -> new CopyOnWriteArraySet<>());
        if (channels.add(subscribeTopic)) {
            subscribeNumber.add(1);
            subscribeTopic.linkSubscribe();
            //MetricManagerHolder.metricManager.getMetricRegistry().getMetricCounter(CounterType.SUBSCRIBE).increment();
        }
    }

    @Override
    public void removeSubscribeTopic(SubscribeTopic subscribeTopic) {
        CopyOnWriteArraySet<SubscribeTopic> channels = topicChannels.computeIfAbsent(subscribeTopic.getTopicFilter(), t -> new CopyOnWriteArraySet<>());
        if (channels.remove(subscribeTopic)) {
            subscribeNumber.add(-1);
            subscribeTopic.unLinkSubscribe();
            //MetricManagerHolder.metricManager.getMetricRegistry().getMetricCounter(CounterType.SUBSCRIBE).decrement();
        }
    }

    @Override
    public int count() {
        return (int) subscribeNumber.sum();
    }

    @Override
    public Set<SubscribeTopic> getAllSubscribesTopic() {
        return topicChannels.values().stream().flatMap(Collection::stream).collect(Collectors.toSet());
    }
}
