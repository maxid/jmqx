package plus.jmqx.broker.mqtt.registry;

import plus.jmqx.broker.mqtt.channel.MqttSession;
import plus.jmqx.broker.mqtt.context.ReceiveContext;
import plus.jmqx.broker.mqtt.registry.impl.Event;

/**
 * 事件注册中心
 *
 * @author maxid
 * @since 2025/4/18 09:18
 */
public interface EventRegistry {

    /**
     * 发送事件
     *
     * @param event   事件类型
     * @param session 会话
     * @param body    事件载荷
     * @param context 上下文
     */
    void registry(Event event, MqttSession session, Object body, ReceiveContext<?> context);

}
