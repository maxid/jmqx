package plus.jmqx.broker.mqtt.registry.impl;

import lombok.extern.slf4j.Slf4j;
import plus.jmqx.broker.mqtt.message.RetainMessage;
import plus.jmqx.broker.mqtt.message.SessionMessage;
import plus.jmqx.broker.mqtt.registry.MessageRegistry;
import plus.jmqx.broker.mqtt.util.TopicRegexUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * MQTT 消息注册中心
 *
 * @author maxid
 * @since 2025/4/16 14:58
 */
@Slf4j
public class DefaultMessageRegistry implements MessageRegistry {

    private final Map<String, Queue<SessionMessage>> sessionMessages = new ConcurrentHashMap<>();

    private final Map<String, RetainMessage> retainMessages     = new ConcurrentHashMap<>();
    private final Map<String, Pattern>       retainPatternCache = new ConcurrentHashMap<>();

    /**
     * 总离线消息计数器
     */
    private final AtomicLong totalOfflineCounter = new AtomicLong(0);

    // ========== 可配置上限（0 = 不限制） ==========

    private volatile int  maxOfflineQueueSize     = 0;
    private volatile long maxTotalOfflineMessages = 0L;
    private volatile long maxRetainMessageCount   = 0L;

    /**
     * 构造消息注册中心
     */
    public DefaultMessageRegistry() {
    }

    // ========== 上限配置 ==========

    /**
     * 设置每客户端离线消息队列上限
     *
     * @param maxOfflineQueueSize 上限，0=不限制
     */
    public void setMaxOfflineQueueSize(int maxOfflineQueueSize) {
        this.maxOfflineQueueSize = maxOfflineQueueSize;
    }

    /**
     * 设置总离线消息存储上限
     *
     * @param maxTotalOfflineMessages 上限，0=不限制
     */
    public void setMaxTotalOfflineMessages(long maxTotalOfflineMessages) {
        this.maxTotalOfflineMessages = maxTotalOfflineMessages;
    }

    /**
     * 设置保留消息总数上限
     *
     * @param maxRetainMessageCount 上限，0=不限制
     */
    public void setMaxRetainMessageCount(long maxRetainMessageCount) {
        this.maxRetainMessageCount = maxRetainMessageCount;
    }

    /**
     * 获取并清空会话消息列表
     *
     * @param clientIdentifier 客户端标识
     * @return 会话消息列表
     */
    @Override
    public List<SessionMessage> getSessionMessage(String clientIdentifier) {
        Queue<SessionMessage> queue = sessionMessages.remove(clientIdentifier);
        if (queue == null || queue.isEmpty()) {
            return Collections.emptyList();
        }
        List<SessionMessage> messages = new ArrayList<>(queue.size());
        SessionMessage message;
        while ((message = queue.poll()) != null) {
            messages.add(message);
        }
        // 扣减总计数
        if (maxTotalOfflineMessages > 0) {
            totalOfflineCounter.addAndGet(-messages.size());
        }
        return messages;
    }

    /**
     * 保存会话消息（受上限约束）
     *
     * @param sessionMessage 会话消息
     */
    @Override
    public void saveSessionMessage(SessionMessage sessionMessage) {
        // 总存储上限检查
        if (maxTotalOfflineMessages > 0) {
            long current = totalOfflineCounter.incrementAndGet();
            if (current > maxTotalOfflineMessages) {
                totalOfflineCounter.decrementAndGet();
                log.warn("total offline msg store full ({}), dropping for [{}]",
                        maxTotalOfflineMessages, sessionMessage.getClientId());
                return;
            }
        }
        // 每队列上限检查
        if (maxOfflineQueueSize > 0) {
            Queue<SessionMessage> queue = sessionMessages.computeIfAbsent(
                    sessionMessage.getClientId(), k -> new ConcurrentLinkedQueue<>());
            if (queue.size() >= maxOfflineQueueSize) {
                SessionMessage dropped = queue.poll();
                log.warn("offline msg queue full for [{}], limit={}, dropping oldest",
                        sessionMessage.getClientId(), maxOfflineQueueSize);
                if (maxTotalOfflineMessages > 0) {
                    totalOfflineCounter.decrementAndGet();
                }
            }
            queue.add(sessionMessage);
        } else {
            // 无上限的原行为
            Queue<SessionMessage> queue = sessionMessages.computeIfAbsent(
                    sessionMessage.getClientId(), k -> new ConcurrentLinkedQueue<>());
            queue.add(sessionMessage);
        }
    }

    /**
     * 保存保留消息（受总数上限约束）
     *
     * @param retainMessage 保留消息
     */
    @Override
    public void saveRetainMessage(RetainMessage retainMessage) {
        if (retainMessage.getBody() == null || retainMessage.getBody().length == 0) {
            retainMessages.remove(retainMessage.getTopic());
            return;
        }
        // 仅新增时检查上限（替换已存在的不计新增）
        if (maxRetainMessageCount > 0
                && !retainMessages.containsKey(retainMessage.getTopic())
                && retainMessages.size() >= maxRetainMessageCount) {
            log.warn("retain store full ({}), rejecting {}", maxRetainMessageCount, retainMessage.getTopic());
            return;
        }
        retainMessages.put(retainMessage.getTopic(), retainMessage);
    }

    /**
     * 根据主题过滤器获取保留消息
     *
     * @param topic 主题或过滤器
     * @return 保留消息列表
     */
    @Override
    public List<RetainMessage> getRetainMessage(String topic) {
        if (retainMessages.isEmpty()) {
            return Collections.emptyList();
        }
        if (!topic.contains("+") && !topic.contains("#")) {
            RetainMessage retainMessage = retainMessages.get(topic);
            return retainMessage == null ? Collections.emptyList() : Collections.singletonList(retainMessage);
        }
        Pattern pattern = retainPattern(topic);
        return retainMessages.entrySet()
                .stream()
                .filter(entry -> pattern.matcher(entry.getKey()).matches())
                .map(Map.Entry::getValue)
                .collect(Collectors.toList());
    }

    /**
     * 获取并缓存保留消息匹配正则
     *
     * @param topicFilter 主题过滤器
     * @return 正则模式
     */
    private Pattern retainPattern(String topicFilter) {
        if (retainPatternCache.size() > 2048) {
            retainPatternCache.clear();
        }
        return retainPatternCache.computeIfAbsent(topicFilter, filter -> Pattern.compile(TopicRegexUtils.regexTopic(filter)));
    }

}
