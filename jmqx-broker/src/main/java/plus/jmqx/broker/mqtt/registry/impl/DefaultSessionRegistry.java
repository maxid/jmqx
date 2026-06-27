package plus.jmqx.broker.mqtt.registry.impl;

import plus.jmqx.broker.mqtt.channel.MqttSession;
import plus.jmqx.broker.mqtt.channel.SessionStatus;
import plus.jmqx.broker.mqtt.registry.SessionRegistry;

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

    /**
     * 最大连接数限制，0=不限制
     */
    private volatile int maxConnections = 0;

    /**
     * 创建默认会话注册中心
     */
    public DefaultSessionRegistry() {
    }

    /**
     * 设置最大连接数
     *
     * @param maxConnections 最大连接数（0=不限制）
     */
    public void setMaxConnections(int maxConnections) {
        this.maxConnections = maxConnections;
    }

    /**
     * 判断是否还有连接容量
     *
     * @return true 允许连接，false 已满
     */
    public boolean hasCapacity() {
        return maxConnections <= 0 || sessions.size() < maxConnections;
    }

    /**
     * 获取当前连接数
     *
     * @return 当前连接数
     */
    public int currentCount() {
        return sessions.size();
    }

    /**
     * 获取最大连接数
     *
     * @return 最大连接数
     */
    public int getMaxConnections() {
        return maxConnections;
    }

    /**
     * 关闭并移除会话
     *
     * @param session 会话
     */
    @Override
    public void close(MqttSession session) {
        Optional.ofNullable(session.getClientId()).ifPresent(sessions::remove);
    }

    /**
     * 注册会话
     *
     * @param clientId 客户端标识
     * @param session  会话
     */
    @Override
    public void registry(String clientId, MqttSession session) {
        sessions.put(clientId, session);
    }

    /**
     * 判断会话是否在线
     *
     * @param clientId 客户端标识
     * @return 是否在线
     */
    @Override
    public boolean exists(String clientId) {
        MqttSession session = sessions.get(clientId);
        return session != null && session.getStatus() == SessionStatus.ONLINE;
    }

    /**
     * 获取会话
     *
     * @param clientId 客户端标识
     * @return 会话
     */
    @Override
    public MqttSession get(String clientId) {
        return sessions.get(clientId);
    }

    /**
     * 统计会话数量
     *
     * @return 会话数量
     */
    @Override
    public Integer counts() {
        return sessions.size();
    }

    /**
     * 获取全部会话集合
     *
     * @return 会话集合
     */
    @Override
    public Collection<MqttSession> getChannels() {
        return sessions.values();
    }

}
