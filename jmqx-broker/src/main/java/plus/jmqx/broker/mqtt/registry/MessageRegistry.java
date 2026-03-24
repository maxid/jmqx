package plus.jmqx.broker.mqtt.registry;

import plus.jmqx.broker.mqtt.message.RetainMessage;
import plus.jmqx.broker.mqtt.message.SessionMessage;
import plus.jmqx.broker.spi.DynamicLoader;

import java.util.List;

/**
 * 消息持久化处理
 *
 * @author maxid
 * @since 2025/4/9 14:17
 */
public interface MessageRegistry extends Startup {

    MessageRegistry INSTANCE = DynamicLoader.findFirst(MessageRegistry.class).orElse(null);

    /**
     * 获取连接下线后的会话消息
     *
     * @param clientId 设备 ID
     * @return 会话消息列表
     */
    List<SessionMessage> getSessionMessage(String clientId);

    /**
     * 发送连接下线后的会话消息
     *
     * @param sessionMessage 会话消息
     */
    void saveSessionMessage(SessionMessage sessionMessage);

    /**
     * 保存 Topic 保留消息
     *
     * @param retainMessage 保留消息
     */
    void saveRetainMessage(RetainMessage retainMessage);

    /**
     * 获取 Topic 保留消息
     *
     * @param topic 主题或过滤器
     * @return 保留消息列表
     */
    List<RetainMessage> getRetainMessage(String topic);

}
