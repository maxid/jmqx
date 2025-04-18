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

    /**
     * 连接鉴权
     *
     * @param clientId 设备 ID
     * @param username 用户名
     * @param password 密码
     * @return 是否鉴权通过
     */
    boolean auth(String clientId, String username, byte[] password);
}
