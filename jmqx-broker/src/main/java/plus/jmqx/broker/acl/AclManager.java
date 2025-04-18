package plus.jmqx.broker.acl;

import plus.jmqx.broker.mqtt.channel.MqttSession;
import plus.jmqx.broker.spi.DynamicLoader;

/**
 * 主题访问控制列表管理
 *
 * @author maxid
 * @since 2025/4/8 17:39
 */
public interface AclManager {
    AclManager INSTANCE = DynamicLoader.findFirst(AclManager.class).orElse(null);

    /**
     * 主题访问控制权限校验
     *
     * @param session 会话
     * @param topic   主题名称
     * @param action  校验权限
     * @return 是否具备指定权限
     */
    boolean check(MqttSession session, String topic, AclAction action);
}
