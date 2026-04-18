package plus.jmqx.broker.mqtt.registry.impl;

import plus.jmqx.broker.mqtt.registry.MessageRegistry;
import plus.jmqx.broker.mqtt.message.RetainMessage;
import plus.jmqx.broker.mqtt.message.SessionMessage;
import plus.jmqx.broker.mqtt.util.TopicRegexUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * MQTT 消息注册中心
 *
 * @author maxid
 * @since 2025/4/16 14:58
 */
public class DefaultMessageRegistry implements MessageRegistry {

    private final Map<String, Queue<SessionMessage>> sessionMessages = new ConcurrentHashMap<>();

    private final Map<String, RetainMessage> retainMessages = new ConcurrentHashMap<>();
    private final Map<String, Pattern> retainPatternCache = new ConcurrentHashMap<>();

    /**
     * 获取并清空会话消息列表。
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
        return messages;
    }

    /**
     * 保存会话消息。
     *
     * @param sessionMessage 会话消息
     */
    @Override
    public void saveSessionMessage(SessionMessage sessionMessage) {
        Queue<SessionMessage> queue = sessionMessages.computeIfAbsent(sessionMessage.getClientId(), key -> new ConcurrentLinkedQueue<>());
        queue.add(sessionMessage);
    }

    /**
     * 保存保留消息。
     *
     * @param retainMessage 保留消息
     */
    @Override
    public void saveRetainMessage(RetainMessage retainMessage) {
        if(retainMessage.getBody() == null || retainMessage.getBody().length == 0) {
            retainMessages.remove(retainMessage.getTopic());
        } else {
            retainMessages.put(retainMessage.getTopic(), retainMessage);
        }
    }

    /**
     * 根据主题过滤器获取保留消息。
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
     * 获取并缓存保留消息匹配正则。
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
