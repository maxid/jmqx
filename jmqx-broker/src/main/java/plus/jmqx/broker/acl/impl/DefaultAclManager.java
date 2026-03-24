package plus.jmqx.broker.acl.impl;

import plus.jmqx.broker.acl.AclAction;
import plus.jmqx.broker.acl.AclManager;
import plus.jmqx.broker.mqtt.channel.MqttSession;

/**
 * 默认主题访问控制列表管理，默认拥有任意主题访问权限
 *
 * @author maxid
 * @since 2025/4/16 15:49
 */
public class DefaultAclManager implements AclManager {

    /**
     * 校验会话对主题的访问权限。
     *
     * @param session 会话
     * @param topic   主题
     * @param action  权限动作
     * @return 是否允许访问
     */
    @Override
    public boolean check(MqttSession session, String topic, AclAction action) {
        return Boolean.TRUE;
    }

}
