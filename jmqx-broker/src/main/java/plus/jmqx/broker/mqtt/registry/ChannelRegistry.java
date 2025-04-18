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
public interface ChannelRegistry extends Startup {
    ChannelRegistry INSTANCE = DynamicLoader.findFirst(ChannelRegistry.class).orElse(null);

    /**
     * 关闭通道
     *
     * @param mqttChannel {@link MqttSession}
     */
    void close(MqttSession mqttChannel);

    /**
     * 注册通道
     *
     * @param clientId 客户端 ID
     * @param mqttChannel      {@link MqttSession}
     */
    void registry(String clientId, MqttSession mqttChannel);

    /**
     * 判读通道是否存在
     *
     * @param clientId 客户端 ID
     * @return 布尔
     */
    boolean exists(String clientId);

    /**
     * 获取通道
     *
     * @param clientId 客户端 ID
     * @return MqttChannel
     */
    MqttSession get(String clientId);

    /**
     * 获取通道计数
     *
     * @return 通道数
     */
    Integer counts();

    /**
     * 获取说有channel信息
     *
     * @return {@link Collection}
     */
    Collection<MqttSession> getChannels();
}
