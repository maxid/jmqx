package plus.jmqx.broker.mqtt.registry;

import plus.jmqx.broker.mqtt.channel.MqttSession;
import plus.jmqx.broker.spi.DynamicLoader;

import java.util.Collection;

/**
 * 会话注册中心
 *
 * @author maxid
 * @since 2025/4/9 12:00
 */
public interface SessionRegistry extends Startup {

    SessionRegistry INSTANCE = DynamicLoader.findFirst(SessionRegistry.class).orElse(null);

    /**
     * 关闭会话
     *
     * @param session 会话
     */
    void close(MqttSession session);

    /**
     * 注册会话
     *
     * @param clientId 客户端 ID
     * @param session  会话
     */
    void registry(String clientId, MqttSession session);

    /**
     * 判读会话是否存在
     *
     * @param clientId 客户端 ID
     * @return 是否存在
     */
    boolean exists(String clientId);

    /**
     * 获取会话
     *
     * @param clientId 客户端 ID
     * @return 会话
     */
    MqttSession get(String clientId);

    /**
     * 获取会话计数
     *
     * @return 会话数量
     */
    Integer counts();

    /**
     * 获取说有channel信息
     *
     * @return 会话集合
     */
    Collection<MqttSession> getChannels();

}
