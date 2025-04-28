package plus.jmqx.example.broker.auth;

import cn.hutool.core.util.StrUtil;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import plus.jmqx.broker.auth.AuthManager;

import java.nio.charset.StandardCharsets;

/**
 * MQTT设备权限管理
 *
 * @author maxid
 * @since 2025/4/26 10:48
 */
@Component
public class PlatformAuthManager implements AuthManager {

    @Value(value = "${jmqx.auth.fixed.username:jmqx}")
    private String username;

    @Value(value = "${jmqx.auth.fixed.password:jmqx}")
    private String password;

    @Override
    public boolean auth(String clientId, String username, byte[] password) {
        String pwd = new String(password, StandardCharsets.UTF_8);
        if (StrUtil.isAllNotEmpty(username, pwd)) {
            return this.username.equals(username) && this.password.equals(pwd);
        }
        return false;
    }
}
