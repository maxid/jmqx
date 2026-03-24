package plus.jmqx.example.broker.acl;

import org.springframework.stereotype.Component;
import plus.jmqx.broker.acl.AclAction;
import plus.jmqx.broker.acl.AclManager;
import plus.jmqx.broker.mqtt.channel.MqttSession;

/**
 * 主题访问控制管理
 *
 * @author maxid
 * @since 2025/4/26 10:47
 */
@Component
public class PlatformAclManager implements AclManager {

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
        return true;
    }

}
