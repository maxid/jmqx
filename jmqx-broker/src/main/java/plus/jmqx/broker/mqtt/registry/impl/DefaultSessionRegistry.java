package plus.jmqx.broker.mqtt.registry.impl;

import plus.jmqx.broker.mqtt.registry.SessionRegistry;
import plus.jmqx.broker.mqtt.channel.SessionStatus;
import plus.jmqx.broker.mqtt.channel.MqttSession;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 会话注册中心
 *
 * @author maxid
 * @since 2025/4/16 14:09
 */
public class DefaultSessionRegistry implements SessionRegistry {
    private final Map<String, MqttSession> sessions = new ConcurrentHashMap<>();

    public DefaultSessionRegistry() {
    }

    @Override
    public void close(MqttSession session) {
        Optional.ofNullable(session.getClientId()).ifPresent(sessions::remove);
    }

    @Override
    public void registry(String clientId, MqttSession session) {
        sessions.put(clientId, session);
    }

    @Override
    public boolean exists(String clientId) {
        MqttSession session = sessions.get(clientId);
        return session != null && session.getStatus() == SessionStatus.ONLINE;
    }

    @Override
    public MqttSession get(String clientId) {
        return sessions.get(clientId);
    }

    @Override
    public Integer counts() {
        return sessions.size();
    }

    @Override
    public Collection<MqttSession> getChannels() {
        return sessions.values();
    }
}
