package plus.jmqx.broker.mqtt.registry.impl;

import plus.jmqx.broker.mqtt.registry.ChannelRegistry;
import plus.jmqx.broker.mqtt.channel.ChannelStatus;
import plus.jmqx.broker.mqtt.channel.MqttChannel;

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
    private final Map<String, MqttChannel> sessions = new ConcurrentHashMap<>();

    public DefaultChannelRegistry() {
    }

    @Override
    public void close(MqttChannel mqttChannel) {
        Optional.ofNullable(mqttChannel.getClientId()).ifPresent(sessions::remove);
    }

    @Override
    public void registry(String clientIdentifier, MqttChannel mqttChannel) {
        sessions.put(clientIdentifier, mqttChannel);
    }

    @Override
    public boolean exists(String clientIdentifier) {
        return sessions.containsKey(clientIdentifier) && sessions.get(clientIdentifier).getStatus() == ChannelStatus.ONLINE;
    }

    @Override
    public MqttChannel get(String clientIdentifier) {
        return sessions.get(clientIdentifier);
    }

    @Override
    public Integer counts() {
        return sessions.size();
    }

    @Override
    public Collection<MqttChannel> getChannels() {
        return sessions.values();
    }
}
