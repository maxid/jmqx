package plus.jmqx.broker.mqtt.registry.impl;

import plus.jmqx.broker.mqtt.registry.MessageRegistry;
import plus.jmqx.broker.mqtt.message.RetainMessage;
import plus.jmqx.broker.mqtt.message.SessionMessage;
import plus.jmqx.broker.mqtt.util.TopicRegexUtils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

/**
 * MQTT 消息注册中心
 *
 * @author maxid
 * @since 2025/4/16 14:58
 */
public class DefaultMessageRegistry implements MessageRegistry {

    private final Map<String, List<SessionMessage>> sessionMessages = new ConcurrentHashMap<>();

    private final Map<String, RetainMessage> retainMessages = new ConcurrentHashMap<>();

    @Override
    public List<SessionMessage> getSessionMessage(String clientIdentifier) {
        return sessionMessages.remove(clientIdentifier);
    }

    @Override
    public void saveSessionMessage(SessionMessage sessionMessage) {
        List<SessionMessage> sessionList = sessionMessages.computeIfAbsent(sessionMessage.getClientId(), key -> new CopyOnWriteArrayList<>());
        sessionList.add(sessionMessage);
    }

    @Override
    public void saveRetainMessage(RetainMessage retainMessage) {
        retainMessages.put(retainMessage.getTopic(), retainMessage);
    }

    @Override
    public List<RetainMessage> getRetainMessage(String topic) {
        return retainMessages.keySet()
                .stream()
                .filter(key -> key.matches(TopicRegexUtils.regexTopic(topic)))
                .map(retainMessages::get)
                .collect(Collectors.toList());
    }
}
