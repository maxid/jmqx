package plus.jmqx.broker.mqtt.topic.impl;

import io.netty.handler.codec.mqtt.MqttQoS;
import plus.jmqx.broker.mqtt.channel.MqttSession;
import plus.jmqx.broker.mqtt.topic.SubscribeTopic;
import plus.jmqx.broker.mqtt.topic.TopicFilter;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

/**
 * 固定主题过滤器
 *
 * @author maxid
 * @since 2025/4/16 10:16
 */
public class FixedTopicFilter implements TopicFilter {

    private final LongAdder subscribeNumber = new LongAdder();

    private final Map<String, Set<SubscribeTopic>> topicChannels = new ConcurrentHashMap<>();

    /**
     * 根据固定主题获取订阅集合。
     *
     * @param topic   主题
     * @param mqttQoS QoS
     * @return 订阅集合
     */
    @Override
    public Set<SubscribeTopic> getSubscribeByTopic(String topic, MqttQoS mqttQoS) {
        Set<SubscribeTopic> channels = topicChannels.get(topic);
        if (channels == null || channels.isEmpty()) {
            return Collections.emptySet();
        }
        return channels.stream().map(tp -> tp.compareQos(mqttQoS)).collect(Collectors.toSet());
    }

    /**
     * 注册固定主题订阅。
     *
     * @param topicFilter 主题过滤器
     * @param mqttChannel 会话
     * @param mqttQoS     QoS
     */
    @Override
    public void addSubscribeTopic(String topicFilter, MqttSession mqttChannel, MqttQoS mqttQoS) {
        this.addSubscribeTopic(new SubscribeTopic(topicFilter, mqttQoS, mqttChannel));
    }

    /**
     * 注册订阅对象。
     *
     * @param subscribeTopic 订阅对象
     */
    @Override
    public void addSubscribeTopic(SubscribeTopic subscribeTopic) {
        Set<SubscribeTopic> channels = topicChannels.computeIfAbsent(subscribeTopic.getTopicFilter(), t -> ConcurrentHashMap.newKeySet());
        if (channels.add(subscribeTopic)) {
            subscribeNumber.add(1);
            subscribeTopic.linkSubscribe();
            //MetricManagerHolder.metricManager.getMetricRegistry().getMetricCounter(CounterType.SUBSCRIBE).increment();
        }
    }

    /**
     * 移除订阅对象。
     *
     * @param subscribeTopic 订阅对象
     */
    @Override
    public void removeSubscribeTopic(SubscribeTopic subscribeTopic) {
        Set<SubscribeTopic> channels = topicChannels.computeIfAbsent(subscribeTopic.getTopicFilter(), t -> ConcurrentHashMap.newKeySet());
        if (channels.remove(subscribeTopic)) {
            subscribeNumber.add(-1);
            subscribeTopic.unLinkSubscribe();
            //MetricManagerHolder.metricManager.getMetricRegistry().getMetricCounter(CounterType.SUBSCRIBE).decrement();
        }
    }

    /**
     * 获取订阅数量。
     *
     * @return 订阅数量
     */
    @Override
    public int count() {
        return (int) subscribeNumber.sum();
    }

    /**
     * 获取全部订阅集合。
     *
     * @return 订阅集合
     */
    @Override
    public Set<SubscribeTopic> getAllSubscribesTopic() {
        return topicChannels.values().stream().flatMap(Collection::stream).collect(Collectors.toSet());
    }

}
