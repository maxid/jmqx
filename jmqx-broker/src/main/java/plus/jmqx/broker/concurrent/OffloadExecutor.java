package plus.jmqx.broker.concurrent;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import plus.jmqx.broker.metrics.MetricsManagerHolder;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * 阻塞调用卸载执行器<br/>
 * 将可能阻塞的同步回调（鉴权、ACL、远程调用等）从 Reactor/Netty IO 线程切到独立业务线程池，
 * 并统一处理超时、队列满拒绝与指标上报。
 *
 * @author maxid
 * @since 2026/7/23 23:30
 */
@Slf4j
public class OffloadExecutor {

    private static final AtomicInteger EXECUTOR_INDEX = new AtomicInteger(1);

    private final Executor          executor;
    private final long              timeoutMillis;
    private final String            namespace;
    private final String            name;
    private final String            queueFullMetric;

    /**
     * 构造卸载执行器
     *
     * @param name             执行器名称（用于日志与线程名前缀，如 auth/acl）
     * @param namespace        命名空间
     * @param timeoutMillis    超时时间（毫秒）
     * @param threadSize       线程池大小
     * @param queueSize        队列大小
     * @param defaultThreads   默认线程数
     * @param defaultQueueSize 默认队列大小
     * @param queueFullMetric  队列满时上报的指标名
     */
    public OffloadExecutor(String name,
                           String namespace,
                           long timeoutMillis,
                           Integer threadSize,
                           Integer queueSize,
                           int defaultThreads,
                           int defaultQueueSize,
                           String queueFullMetric) {
        this.name = name;
        this.namespace = namespace;
        this.timeoutMillis = Math.max(timeoutMillis, 1L);
        this.queueFullMetric = queueFullMetric;
        this.executor = createExecutor(name, threadSize, queueSize, defaultThreads, defaultQueueSize);
    }

    /**
     * 异步执行任务；超时、异常、队列满时返回 fallback
     *
     * @param task     任务
     * @param fallback 失败回退值
     * @param traceKey 日志追踪键（如 clientId）
     * @param <T>      结果类型
     * @return 异步结果
     */
    public <T> CompletableFuture<T> supply(Supplier<T> task, T fallback, String traceKey) {
        try {
            CompletableFuture<T> source = CompletableFuture.supplyAsync(task, executor);
            return source
                    .completeOnTimeout(fallback, timeoutMillis, TimeUnit.MILLISECONDS)
                    .exceptionally(ex -> fallback);
        } catch (RejectedExecutionException e) {
            log.warn("[{}] {} queue full (size={}), rejecting [{}]",
                    namespace, name, ((ThreadPoolExecutor) executor).getQueue().size(), traceKey);
            MetricsManagerHolder.get().recordDroppedMessage(queueFullMetric);
            return CompletableFuture.completedFuture(fallback);
        }
    }

    private static Executor createExecutor(String name,
                                           Integer threadSize,
                                           Integer queueSize,
                                           int defaultThreads,
                                           int defaultQueueSize) {
        int threads = normalize(threadSize, defaultThreads);
        int queue = normalize(queueSize, defaultQueueSize);
        int index = EXECUTOR_INDEX.getAndIncrement();
        ThreadFactory factory = new PrefixedThreadFactory("jmqx-" + name + "-io-" + index + "-");
        return new ThreadPoolExecutor(
                threads,
                threads,
                60L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(queue),
                factory,
                new ThreadPoolExecutor.AbortPolicy()
        );
    }

    private static int normalize(Integer value, int fallback) {
        return value == null || value <= 0 ? fallback : value;
    }

    private static class PrefixedThreadFactory implements ThreadFactory {

        private final AtomicInteger sequence = new AtomicInteger(1);
        private final String        prefix;

        private PrefixedThreadFactory(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public Thread newThread(@NonNull Runnable r) {
            Thread thread = new Thread(r, prefix + sequence.getAndIncrement());
            thread.setDaemon(true);
            return thread;
        }
    }

}
