package plus.jmqx.broker.mqtt.registry.impl;

import io.netty.handler.codec.mqtt.MqttQoS;
import lombok.extern.slf4j.Slf4j;
import plus.jmqx.broker.mqtt.channel.MqttSession;
import plus.jmqx.broker.mqtt.topic.SubscribeTopic;
import plus.jmqx.broker.mqtt.topic.TopicFilter;
import plus.jmqx.broker.mqtt.registry.TopicRegistry;
import plus.jmqx.broker.mqtt.topic.impl.FixedTopicFilter;
import plus.jmqx.broker.mqtt.topic.impl.TreeTopicFilter;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * MQTT 默认主题注册中心
 *
 * @author maxid
 * @since 2025/4/16 09:22
 */
@Slf4j
public class DefaultTopicRegistry implements TopicRegistry {
    /**
     * 单级匹配
     */
    private static final String SINGLE_SYMBOL = "+";
    /**
     * 多级匹配
     */
    private static final String MULTI_SYMBOL = "#";
    /**
     * 固定主题过滤器
     */
    private final TopicFilter fixedTopicFilter;
    /**
     * 多级主题过滤器
     */
    private final TopicFilter treeTopicFilter;

    public DefaultTopicRegistry() {
        this.fixedTopicFilter = new FixedTopicFilter();
        this.treeTopicFilter = new TreeTopicFilter();
    }

    @Override
    public void registrySubscribeTopic(String topicFilter, MqttSession session, MqttQoS qos) {
        this.registrySubscribeTopic(new SubscribeTopic(topicFilter, qos, session));
    }

    @Override
    public void registrySubscribeTopic(SubscribeTopic subscribeTopic) {
        if (subscribeTopic.getTopicFilter().contains(SINGLE_SYMBOL) || subscribeTopic.getTopicFilter().contains(MULTI_SYMBOL)) {
            treeTopicFilter.addSubscribeTopic(subscribeTopic);
        } else {
            fixedTopicFilter.addSubscribeTopic(subscribeTopic);
        }
    }

    @Override
    public void clear(MqttSession mqttChannel) {
        Set<SubscribeTopic> topics = mqttChannel.getTopics();
        if (log.isDebugEnabled()) {
            log.debug("mqttChannel channel {} clear topics {}", mqttChannel, topics);
        }
        topics.forEach(this::removeSubscribeTopic);
    }

    @Override
    public void removeSubscribeTopic(SubscribeTopic subscribeTopic) {
        if (subscribeTopic.getTopicFilter().contains(SINGLE_SYMBOL) || subscribeTopic.getTopicFilter().contains(MULTI_SYMBOL)) {
            treeTopicFilter.removeSubscribeTopic(subscribeTopic);
        } else {
            fixedTopicFilter.removeSubscribeTopic(subscribeTopic);
        }
    }

    @Override
    public Set<SubscribeTopic> getSubscribesByTopic(String topicName, MqttQoS qos) {
        Set<SubscribeTopic> subscribeTopics = fixedTopicFilter.getSubscribeByTopic(topicName, qos);
        subscribeTopics.addAll(treeTopicFilter.getSubscribeByTopic(topicName, qos));
        return subscribeTopics;
    }

    @Override
    public void registrySubscribesTopic(Set<SubscribeTopic> mqttTopicSubscriptions) {
        mqttTopicSubscriptions.forEach(this::registrySubscribeTopic);
    }

    @Override
    public Map<String, Set<MqttSession>> getAllTopics() {
        Set<SubscribeTopic> subscribeTopics = fixedTopicFilter.getAllSubscribesTopic();
        subscribeTopics.addAll(treeTopicFilter.getAllSubscribesTopic());
        return subscribeTopics
                .stream()
                .collect(Collectors.groupingBy(
                        SubscribeTopic::getTopicFilter,
                        Collectors.mapping(SubscribeTopic::getSession, Collectors.toSet())));
    }

    @Override
    public Integer counts() {
        return fixedTopicFilter.count() + treeTopicFilter.count();
    }

}
