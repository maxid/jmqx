package plus.jmqx.broker.auth.impl;

import plus.jmqx.broker.auth.AuthManager;

/**
 * 默认设备鉴权管理器，默认任意设备都鉴权通过
 *
 * @author maxid
 * @since 2025/4/16 15:50
 */
public class DefaultAuthManager implements AuthManager {
    @Override
    public boolean auth(String clientId, String username, byte[] password) {
        return Boolean.TRUE;
    }
}
