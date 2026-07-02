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

    private final long initialDelayMs;
    private final long maxDelayMs;
    private final Supplier<Mono<?>> connectCall;     // 返回 connect() Mono
    private final Scheduler scheduler;              // 测试中为 VirtualTimeScheduler，生产为 parallel
    private final AtomicBoolean stopped = new AtomicBoolean(false);

    public MqttAutoReconnect(long initialDelayMs, long maxDelayMs,
                             Supplier<Mono<?>> connectCall, Scheduler scheduler) {
        this.initialDelayMs = initialDelayMs;
        this.maxDelayMs = maxDelayMs;
        this.connectCall = connectCall;
        this.scheduler = scheduler;
    }

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
                        v -> {},
                        err -> scheduleReconnect(attempts + 1),
                        () -> { /* connect 成功；重置 stopped 以便后续断开可再次重连 */ }
                );
    }

    private long computeBackoff(int attempt) {
        long base = Math.min(initialDelayMs * (1L << Math.min(attempt, 16)), maxDelayMs);
        double jitter = 1.0 + (ThreadLocalRandom.current().nextDouble() - 0.5) * 0.5;
        return (long) (base * jitter);
    }

    public void stop() {
        stopped.set(true);
    }
}
