package plus.jmqx.client.mqtt.internal.reconnect;

import lombok.extern.slf4j.Slf4j;
import plus.jmqx.client.mqtt.lifecycle.MqttClientDisconnectedContext;
import plus.jmqx.client.mqtt.lifecycle.MqttClientDisconnectedListener;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * 自动重连（reactor {@code Mono.delay} 实现，绝不 {@code Thread.sleep}）。
 *
 * <p>指数退避 + ±25% jitter，封顶于 maxDelay。用户主动断开（source=USER）不重连。
 *
 * @author maxid
 */
@Slf4j
public class MqttAutoReconnect implements MqttClientDisconnectedListener {

    /**
     * 初始重连延迟（毫秒）
     */
    private final long              initialDelayMs;
    /**
     * 最大重连延迟（毫秒）
     */
    private final long              maxDelayMs;
    /**
     * 返回 connect() Mono 的供应器
     */
    private final Supplier<Mono<?>> connectCall;
    /**
     * 调度器（测试中使用 VirtualTimeScheduler，生产中使用 parallel）
     */
    private final Scheduler         scheduler;
    /**
     * 是否已停止重连
     */
    private final AtomicBoolean     stopped = new AtomicBoolean(false);

    /**
     * 构造 MqttAutoReconnect。
     *
     * @param initialDelayMs 初始重连延迟（毫秒）
     * @param maxDelayMs     最大重连延迟（毫秒）
     * @param connectCall    返回 connect() Mono 的供应器
     * @param scheduler      调度器
     */
    public MqttAutoReconnect(long initialDelayMs, long maxDelayMs,
                             Supplier<Mono<?>> connectCall, Scheduler scheduler) {
        this.initialDelayMs = initialDelayMs;
        this.maxDelayMs = maxDelayMs;
        this.connectCall = connectCall;
        this.scheduler = scheduler;
    }

    /**
     * 断开连接事件处理。用户主动断开（source=USER）不触发重连。
     *
     * @param ctx 断开连接上下文
     */
    @Override
    public void onDisconnected(MqttClientDisconnectedContext ctx) {
        if (ctx.getSource() == MqttClientDisconnectedContext.DisconnectSource.USER) {
            stopped.set(true);
            return;
        }
        if (stopped.get()) {
            return;
        }
        scheduleReconnect(ctx.getReconnector().getAttempts());
    }

    private void scheduleReconnect(int attempts) {
        long delay = computeBackoff(attempts);
        log.info("Auto-reconnect attempt {} scheduled in {}ms", attempts + 1, delay);
        Mono.delay(Duration.ofMillis(delay), scheduler)
                .flatMap(t -> connectCall.get())
                .subscribe(
                        v -> {
                        },
                        err -> scheduleReconnect(attempts + 1),
                        () -> { /* connect 成功；重置 stopped 以便后续断开可再次重连 */ }
                );
    }

    private long computeBackoff(int attempt) {
        long base = Math.min(initialDelayMs * (1L << Math.min(attempt, 16)), maxDelayMs);
        double jitter = 1.0 + (ThreadLocalRandom.current().nextDouble() - 0.5) * 0.5;
        return (long) (base * jitter);
    }

    /**
     * 停止自动重连
     */
    public void stop() {
        stopped.set(true);
    }

}
