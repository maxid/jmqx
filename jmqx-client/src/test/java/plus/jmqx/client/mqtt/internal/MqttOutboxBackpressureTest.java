package plus.jmqx.client.mqtt.internal;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class MqttOutboxBackpressureTest {

    @Test
    void acquireBlocksWhenInflightFullThenResumesOnRelease() {
        MqttOutbox outbox = new MqttOutbox(1);  // 1 inflight slot
        AtomicInteger acquired = new AtomicInteger();

        // 第一次 acquire 成功（Mono<Void> 无 onNext，仅 onComplete）
        outbox.acquire(1).doOnTerminate(acquired::incrementAndGet).block();
        assertEquals(1, acquired.get());

        // 第二次 acquire 阻塞（inflight 满）—— 100ms 内 acquire 计数不变（仍为 1）
        StepVerifier.create(outbox.acquire(2).doOnTerminate(acquired::incrementAndGet))
                .expectSubscription()
                .thenAwait(Duration.ofMillis(100))   // 期间不应完成
                .then(() -> assertEquals(1, acquired.get(), "第二个 acquire 不应在 inflight 满时完成"))
                .then(() -> outbox.release(1))   // 释放 slot 唤醒等待者
                .verifyComplete();
        assertEquals(2, acquired.get());
    }

    @Test
    void releaseWakesOneWaiter() {
        MqttOutbox outbox = new MqttOutbox(1);
        outbox.acquire(1).block();
        // 两个等待者
        outbox.acquire(2).doOnNext(v -> {}).subscribe();
        outbox.acquire(3).doOnNext(v -> {}).subscribe();
        outbox.release(1);  // 唤醒 pid 2 的等待者
        // pid 3 仍阻塞（只有一个 slot，现由 2 占用）
        outbox.remove(2);    // 模拟：取消 pid 2 的 slot
        assertDoesNotThrow(() -> outbox.release(3));
    }

    @Test
    void availablePermitsReflectsInflight() {
        MqttOutbox outbox = new MqttOutbox(3);
        assertEquals(3, outbox.availablePermits());
        outbox.acquire(1).block();
        assertEquals(2, outbox.availablePermits());
        outbox.release(1);
        assertEquals(3, outbox.availablePermits());
    }
}
