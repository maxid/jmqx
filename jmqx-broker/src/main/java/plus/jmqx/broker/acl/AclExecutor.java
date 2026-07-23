package plus.jmqx.broker.acl;

import plus.jmqx.broker.concurrent.OffloadExecutor;
import plus.jmqx.broker.config.Configuration;
import plus.jmqx.broker.mqtt.channel.MqttSession;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * ACL 执行器<br/>
 * 因为不清楚用户的 ACL 实现采用何种方案(如 openfeign 等), 可能会导致 Reactor 业务线程（jmqx-control-io / jmqx-publish-io）被阻塞<br/>
 * 统一把 ACL 调用切到独立业务线程池（与 AuthExecutor 隔离，避免发布风暴饿死连接鉴权），并设置超时和熔断
 *
 * @author maxid
 * @since 2026/7/23 23:30
 */
public class AclExecutor {

    private static final int DEFAULT_ACL_THREADS    = Math.max(Runtime.getRuntime().availableProcessors() * 4, 16);
    private static final int DEFAULT_ACL_QUEUE_SIZE = 200000;

    private final AclManager      aclManager;
    private final OffloadExecutor offloadExecutor;

    /**
     * 构造 ACL 执行器（基于配置对象）
     *
     * @param aclManager ACL 管理器
     * @param config     配置
     */
    public AclExecutor(AclManager aclManager, Configuration config) {
        this(aclManager, config.getClusterConfig().getNamespace(), config.getAclTimeoutMillis(),
                config.getAclThreadSize(), config.getAclQueueSize());
    }

    /**
     * 构造 ACL 执行器（基于明细参数）
     *
     * @param aclManager     ACL 管理器
     * @param namespace      命名空间
     * @param timeoutMillis  ACL 超时时间（毫秒）
     * @param aclThreadSize  ACL 线程池大小
     * @param aclQueueSize   ACL 线程池队列大小
     */
    public AclExecutor(AclManager aclManager, String namespace, long timeoutMillis,
                       Integer aclThreadSize, Integer aclQueueSize) {
        this.aclManager = aclManager;
        this.offloadExecutor = new OffloadExecutor(
                "acl",
                namespace,
                timeoutMillis,
                aclThreadSize,
                aclQueueSize,
                DEFAULT_ACL_THREADS,
                DEFAULT_ACL_QUEUE_SIZE,
                "acl_queue_full"
        );
    }

    /**
     * 执行单次 ACL 校验，超时/异常/队列满时返回 false
     *
     * @param session 会话
     * @param topic   主题
     * @param action  动作
     * @return 校验结果
     */
    public CompletableFuture<Boolean> check(MqttSession session, String topic, AclAction action) {
        String traceKey = session == null ? topic : session.getClientId();
        return offloadExecutor.supply(
                () -> Boolean.TRUE.equals(aclManager.check(session, topic, action)),
                Boolean.FALSE,
                traceKey
        );
    }

    /**
     * 在 ACL 线程池执行自定义任务（如一次 SUBSCRIBE 内批量校验），超时/异常/队列满时返回 fallback
     *
     * @param task     任务
     * @param fallback 失败回退值
     * @param traceKey 日志追踪键
     * @param <T>      结果类型
     * @return 异步结果
     */
    public <T> CompletableFuture<T> supply(Supplier<T> task, T fallback, String traceKey) {
        return offloadExecutor.supply(task, fallback, traceKey);
    }

}
