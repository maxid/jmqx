package plus.jmqx.broker.auth;

import plus.jmqx.broker.concurrent.OffloadExecutor;
import plus.jmqx.broker.config.Configuration;

import java.util.concurrent.CompletableFuture;

/**
 * 鉴权执行器<br/>
 * 因为不清楚用户的鉴权实现采用何种方案(如 openfeign 等), 可能会导致 Netty event loop 被阻塞, 连带影响心跳、收发包和重连风暴<br/>
 * 统一把鉴权调用切到独立业务线程池，并设置超时和熔断
 * <p>
 * 鉴权完成后应通过 {@code scheduleOnControl} 回流控制面，勿在本池线程继续做会话注册或协议写回编排。
 *
 * @author maxid
 * @since 2026/4/16 23:04
 */
public class AuthExecutor {

    private static final int DEFAULT_AUTH_THREADS    = Math.max(Runtime.getRuntime().availableProcessors() * 4, 16);
    private static final int DEFAULT_AUTH_QUEUE_SIZE = 200000;

    private final AuthManager     authManager;
    private final OffloadExecutor offloadExecutor;

    /**
     * 构造鉴权执行器（基于配置对象）
     *
     * @param authManager 鉴权管理器
     * @param config      配置
     */
    public AuthExecutor(AuthManager authManager, Configuration config) {
        this(authManager, config.getClusterConfig().getNamespace(), config.getAuthTimeoutMillis(),
                config.getAuthThreadSize(), config.getAuthQueueSize());
    }

    /**
     * 构造鉴权执行器（基于明细参数）
     *
     * @param authManager    鉴权管理器
     * @param namespace      命名空间
     * @param timeoutMillis  鉴权超时时间（毫秒）
     * @param authThreadSize 鉴权线程池大小
     * @param authQueueSize  鉴权线程池队列大小
     */
    public AuthExecutor(AuthManager authManager, String namespace, long timeoutMillis,
                        Integer authThreadSize, Integer authQueueSize) {
        this.authManager = authManager;
        this.offloadExecutor = new OffloadExecutor(
                "auth",
                namespace,
                timeoutMillis,
                authThreadSize,
                authQueueSize,
                DEFAULT_AUTH_THREADS,
                DEFAULT_AUTH_QUEUE_SIZE,
                "auth_queue_full"
        );
    }

    /**
     * 执行鉴权并在超时/异常/队列满场景下返回失败
     *
     * @param clientId 设备 ID
     * @param username 用户名
     * @param password 密码
     * @return 鉴权结果
     */
    public CompletableFuture<Boolean> execute(String clientId, String username, byte[] password) {
        return offloadExecutor.supply(
                () -> Boolean.TRUE.equals(authManager.auth(clientId, username, password)),
                Boolean.FALSE,
                clientId
        );
    }

}
