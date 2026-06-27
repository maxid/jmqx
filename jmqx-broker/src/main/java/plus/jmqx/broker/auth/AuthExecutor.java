package plus.jmqx.broker.auth;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import plus.jmqx.broker.config.Configuration;
import plus.jmqx.broker.metrics.MetricsManagerHolder;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 鉴权执行器<br/>
 * 因为不清楚用户的鉴权实现采用何种方案(如 openfeign 等), 可能会导致 Netty event loop 被阻塞, 连带影响心跳、收发包和重连风暴<br/>
 * 统一把鉴权调用切到业务线程池（如 boundedElastic / 自定义线程池），并设置超时和熔断
 *
 * @author maxid
 * @since 2026/4/16 23:04
 */
@Slf4j
public class AuthExecutor {

    private static final int           DEFAULT_AUTH_THREADS    = Math.max(Runtime.getRuntime().availableProcessors() * 4, 16);
    private static final int           DEFAULT_AUTH_QUEUE_SIZE = 200000;
    private static final AtomicInteger AUTH_EXECUTOR_INDEX     = new AtomicInteger(1);

    private final AuthManager authManager;
    private final Executor    executor;
    private final long        timeoutMillis;
    private final String      namespace;

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
        this.namespace = namespace;
        this.executor = executor(authThreadSize, authQueueSize);
        this.timeoutMillis = Math.max(timeoutMillis, 1L);
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
        try {
            CompletableFuture<Boolean> source = CompletableFuture.supplyAsync(
                    () -> authManager.auth(clientId, username, password),
                    executor
            );
            return source
                    .thenApply(Boolean.TRUE::equals)
                    .completeOnTimeout(Boolean.FALSE, timeoutMillis, TimeUnit.MILLISECONDS)
                    .exceptionally(ex -> Boolean.FALSE);
        } catch (RejectedExecutionException e) {
            log.warn("[{}] auth queue full (size={}), rejecting [{}]",
                    namespace, ((ThreadPoolExecutor) executor).getQueue().size(), clientId);
            MetricsManagerHolder.get().recordDroppedMessage("auth_queue_full");
            return CompletableFuture.completedFuture(Boolean.FALSE);
        }
    }

    private static Executor executor(Integer authThreadSize, Integer authQueueSize) {
        int threadSize = normalize(authThreadSize, DEFAULT_AUTH_THREADS);
        int queueSize = normalize(authQueueSize, DEFAULT_AUTH_QUEUE_SIZE);
        int index = AUTH_EXECUTOR_INDEX.getAndIncrement();
        ThreadFactory factory = new AuthThreadFactory(index);
        return new ThreadPoolExecutor(
                threadSize,
                threadSize,
                60L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(queueSize),
                factory,
                new ThreadPoolExecutor.AbortPolicy()
        );
    }

    private static int normalize(Integer value, int fallback) {
        return value == null || value <= 0 ? fallback : value;
    }

    private static class AuthThreadFactory implements ThreadFactory {

        private final AtomicInteger sequence = new AtomicInteger(1);
        private final String        prefix;

        private AuthThreadFactory(int index) {
            this.prefix = "jmqx-auth-io-" + index + "-";
        }

        @Override
        public Thread newThread(@NonNull Runnable r) {
            Thread thread = new Thread(r, prefix + sequence.getAndIncrement());
            thread.setDaemon(true);
            return thread;
        }
    }

}
