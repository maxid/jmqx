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
    @Override
    public boolean check(MqttSession session, String topic, AclAction action) {
        return true;
    }
}
