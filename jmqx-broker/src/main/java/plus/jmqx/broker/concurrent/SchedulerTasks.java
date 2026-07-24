package plus.jmqx.broker.concurrent;

import plus.jmqx.broker.mqtt.context.ContextHolder;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * Scheduler 调度辅助<br/>
 * 用于 Auth/ACL Offload 完成后回流数据面，以及集群扩散订阅到专用池。
 *
 * @author maxid
 * @since 2026/7/24
 */
public final class SchedulerTasks {

    private SchedulerTasks() {
    }

    /**
     * 在指定 Scheduler 上执行任务；scheduler 为空时回退到当前线程（便于单测）。
     *
     * @param scheduler 目标调度器
     * @param task      任务
     */
    public static void schedule(Scheduler scheduler, Runnable task) {
        if (scheduler == null) {
            task.run();
            return;
        }
        scheduler.schedule(task);
    }

    /**
     * 将 Mono 订阅切到集群专用池；未初始化时回退全局 boundedElastic（兼容早期启动路径）。
     *
     * @param holder 上下文持有器
     * @param mono   源 Mono
     * @param <T>    元素类型
     * @return 切换后的 Mono
     */
    public static <T> Mono<T> subscribeOnCluster(ContextHolder holder, Mono<T> mono) {
        Scheduler scheduler = holder != null ? holder.getClusterScheduler() : null;
        if (scheduler == null) {
            return mono.subscribeOn(Schedulers.boundedElastic());
        }
        return mono.subscribeOn(scheduler);
    }

    /**
     * 解析线程/队列规模：{@code null} 或 {@code <=0} 时使用 fallback。
     *
     * @param configured 配置值
     * @param fallback   回退值
     * @return 有效规模
     */
    public static int resolveSize(Integer configured, int fallback) {
        return configured == null || configured <= 0 ? fallback : configured;
    }

}
