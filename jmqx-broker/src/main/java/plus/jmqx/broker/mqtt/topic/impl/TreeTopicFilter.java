package plus.jmqx.broker.mqtt.topic.impl;

import io.netty.handler.codec.mqtt.MqttQoS;
import plus.jmqx.broker.mqtt.channel.MqttSession;
import plus.jmqx.broker.mqtt.topic.SubscribeTopic;
import plus.jmqx.broker.mqtt.topic.TopicFilter;
import plus.jmqx.broker.mqtt.topic.TreeNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
    private static final int CACHE_LIMIT = 2048;
    private static final int BUCKET_LIMIT = 256;
    private static final int MAX_BUCKETS = 64;
    private final Map<String, Map<String, List<SubscribeTopic>>> bucketCaches = new ConcurrentHashMap<>();

    /**
     * 根据主题获取订阅集合（带缓存）。
     *
     * @param topic   主题
     * @param mqttQoS QoS
     * @return 订阅集合
     */
    @Override
    public Set<SubscribeTopic> getSubscribeByTopic(String topic, MqttQoS mqttQoS) {
        Map<String, List<SubscribeTopic>> bucket = bucketCache(topic);
        List<SubscribeTopic> matches = bucket.get(topic);
        if (matches == null) {
            matches = rootTreeNode.getSubscribeByTopic(topic);
            bucket.put(topic, Collections.unmodifiableList(new ArrayList<>(matches)));
        }
        return matches.stream().map(tp -> tp.compareQos(mqttQoS)).collect(Collectors.toSet());
    }

    /**
     * 注册主题订阅。
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
     * 注册订阅对象并刷新缓存。
     *
     * @param subscribeTopic 订阅对象
     */
    @Override
    public void addSubscribeTopic(SubscribeTopic subscribeTopic) {
        if (rootTreeNode.addSubscribeTopic(subscribeTopic)) {
            subscribeNumber.add(1);
            //MetricManagerHolder.metricManager.getMetricRegistry().getMetricCounter(CounterType.SUBSCRIBE).increment();
            subscribeTopic.linkSubscribe();
            bucketCaches.clear();
        }
    }

    /**
     * 移除订阅对象并刷新缓存。
     *
     * @param subscribeTopic 订阅对象
     */
    @Override
    public void removeSubscribeTopic(SubscribeTopic subscribeTopic) {
        if (rootTreeNode.removeSubscribeTopic(subscribeTopic)) {
            subscribeNumber.add(-1);
            //MetricManagerHolder.metricManager.getMetricRegistry().getMetricCounter(CounterType.SUBSCRIBE).decrement();
            subscribeTopic.unLinkSubscribe();
            bucketCaches.clear();
        }
    }

    /**
     * 获取主题分桶缓存。
     *
     * @param topic 主题
     * @return 分桶缓存
     */
    private Map<String, List<SubscribeTopic>> bucketCache(String topic) {
        if (bucketCaches.size() > MAX_BUCKETS) {
            bucketCaches.clear();
        }
        String bucketKey = bucketKey(topic);
        return bucketCaches.computeIfAbsent(bucketKey, key -> lruMap(BUCKET_LIMIT));
    }

    /**
     * 计算分桶 key。
     *
     * @param topic 主题
     * @return 分桶 key
     */
    private String bucketKey(String topic) {
        if (topic == null || topic.isEmpty()) {
            return "";
        }
        int index = topic.indexOf('/');
        return index < 0 ? topic : topic.substring(0, index);
    }

    /**
     * 创建 LRU 缓存映射。
     *
     * @param limit 上限
     * @return LRU Map
     */
    private Map<String, List<SubscribeTopic>> lruMap(int limit) {
        return Collections.synchronizedMap(new LinkedHashMap<String, List<SubscribeTopic>>(64, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, List<SubscribeTopic>> eldest) {
                return size() > limit || size() > CACHE_LIMIT;
            }
        });
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
        return rootTreeNode.getAllSubscribesTopic();
    }

}
