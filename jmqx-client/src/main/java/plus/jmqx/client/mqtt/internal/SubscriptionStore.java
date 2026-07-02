package plus.jmqx.client.mqtt.internal;

import lombok.extern.slf4j.Slf4j;
import plus.jmqx.client.mqtt.internal.util.TopicMatcher;
import plus.jmqx.client.mqtt.message.MqttPublish;
import plus.jmqx.client.mqtt.message.MqttTopicFilter;
import reactor.core.publisher.Sinks;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 持有活动订阅。每个订阅持有一个 {@link reactor.core.publisher.Sinks.Many} 用于背压投递。
 *
 * <p>重连时 {@link #snapshotFilters()} 返回所有订阅以便重新 SUBSCRIBE。
 *
 * @author maxid
 */
@Slf4j
public final class SubscriptionStore {

    /**
     * 一个订阅条目：原始过滤器 + 其投递 sink。
     */
    public record Subscription(MqttTopicFilter filter, Sinks.Many<MqttPublish> sink) {
    }

    /**
     * 订阅条目列表（线程安全）
     */
    private final List<Subscription> subscriptions = new CopyOnWriteArrayList<>();

    /**
     * 添加一个订阅。
     *
     * @param filter 主题过滤器
     * @param sink   投递 sink
     * @return 创建的订阅条目
     */
    public Subscription add(MqttTopicFilter filter, Sinks.Many<MqttPublish> sink) {
        Subscription sub = new Subscription(filter, sink);
        subscriptions.add(sub);
        return sub;
    }

    /**
     * 移除匹配任一给定过滤器字符串的订阅。
     *
     * @param filters 要移除的主题过滤器字符串集合
     */
    public void removeAll(Collection<String> filters) {
        subscriptions.removeIf(s -> filters.contains(s.filter().getTopicFilter()));
    }

    /**
     * 将入站 PUBLISH 路由到所有匹配的订阅 sink。
     *
     * @param publish 入站发布消息
     */
    public void route(MqttPublish publish) {
        for (Subscription sub : subscriptions) {
            if (TopicMatcher.matches(sub.filter().getTopicFilter(), publish.getTopic())) {
                sub.sink().tryEmitNext(publish);
            }
        }
    }

    /**
     * 订阅过滤器快照（用于重连重新订阅）。
     *
     * @return 主题过滤器列表
     */
    public List<MqttTopicFilter> snapshotFilters() {
        return subscriptions.stream().map(Subscription::filter).toList();
    }

    public boolean isEmpty() {
        return subscriptions.isEmpty();
    }

    public void clear() {
        subscriptions.clear();
    }

}
