package plus.jmqx.broker.mqtt.registry.impl;

import plus.jmqx.broker.mqtt.registry.ChannelRegistry;
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
public class DefaultChannelRegistry implements ChannelRegistry {
    private final Map<String, MqttSession> sessions = new ConcurrentHashMap<>();

    public DefaultChannelRegistry() {
    }

    @Override
    public void close(MqttSession mqttChannel) {
        Optional.ofNullable(mqttChannel.getClientId()).ifPresent(sessions::remove);
    }

    @Override
    public void registry(String clientIdentifier, MqttSession mqttChannel) {
        sessions.put(clientIdentifier, mqttChannel);
    }

    @Override
    public boolean exists(String clientIdentifier) {
        return sessions.containsKey(clientIdentifier) && sessions.get(clientIdentifier).getStatus() == SessionStatus.ONLINE;
    }

    @Override
    public MqttSession get(String clientIdentifier) {
        return sessions.get(clientIdentifier);
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
