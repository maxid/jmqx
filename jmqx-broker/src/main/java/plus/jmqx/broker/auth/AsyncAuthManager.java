package plus.jmqx.broker.auth;

import reactor.core.publisher.Mono;

/**
 * 异步设备连接鉴权管理
 *
 * @author maxid
 * @since 2026/4/16 23:04
 */
public interface AsyncAuthManager {

    /**
     * 异步连接鉴权
     *
     * @param clientId 设备 ID
     * @param username 用户名
     * @param password 密码
     * @return 鉴权结果
     */
    Mono<Boolean> authAsync(String clientId, String username, byte[] password);

}
