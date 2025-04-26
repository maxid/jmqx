package plus.jmqx.example.broker.auth;

import org.springframework.stereotype.Component;
import plus.jmqx.broker.auth.AuthManager;

/**
 * MQTT设备权限管理
 *
 * @author maxid
 * @since 2025/4/26 10:48
 */
@Component
public class PlatformAuthManager implements AuthManager {
    @Override
    public boolean auth(String clientId, String username, byte[] password) {
        return true;
    }
}
