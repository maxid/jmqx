package plus.jmqx.broker.auth;

import plus.jmqx.broker.spi.DynamicLoader;

/**
 * 设备连接鉴权管理
 *
 * @author maxid
 * @since 2025/4/8 17:39
 */
public interface AuthManager {
    AuthManager INSTANCE = DynamicLoader.findFirst(AuthManager.class).orElse(null);
}
