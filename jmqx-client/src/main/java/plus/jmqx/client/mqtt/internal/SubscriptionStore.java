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

    /** 一个订阅条目：原始过滤器 + 其投递 sink。 */
    public record Subscription(MqttTopicFilter filter, Sinks.Many<MqttPublish> sink) {
    }

    private final List<Subscription> subscriptions = new CopyOnWriteArrayList<>();

    public Subscription add(MqttTopicFilter filter, Sinks.Many<MqttPublish> sink) {
        Subscription sub = new Subscription(filter, sink);
        subscriptions.add(sub);
        return sub;
    }

    /** 移除匹配任一给定过滤器字符串的订阅。 */
    public void removeAll(Collection<String> filters) {
        subscriptions.removeIf(s -> filters.contains(s.filter().getTopicFilter()));
    }

    /** 将入站 PUBLISH 路由到所有匹配的订阅 sink。 */
    public void route(MqttPublish publish) {
        for (Subscription sub : subscriptions) {
            if (TopicMatcher.matches(sub.filter().getTopicFilter(), publish.getTopic())) {
                sub.sink().tryEmitNext(publish);
            }
        }
    }

    /** 订阅过滤器快照（用于重连重新订阅）。 */
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
