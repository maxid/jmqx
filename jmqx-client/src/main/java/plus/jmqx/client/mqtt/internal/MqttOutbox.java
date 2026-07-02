package plus.jmqx.client.mqtt.internal;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 出站 inflight 限制器。
 *
 * <p>将并发未 ACK 的 PUBLISH（QoS1/2）限制在 Receive Maximum（v5）或配置的 maxInflightMessages（v3）。
 * 当 inflight 满时，{@link #acquire(int)} 的 Mono 会挂起（而非 error）—— 对调用方的背压。
 *
 * @author maxid
 */
@Slf4j
public final class MqttOutbox {

    /**
     * 当前可用许可数
     */
    private final    AtomicInteger permits;
    /**
     * 最大许可数上限
     */
    private volatile int           maxPermits;
    /**
     * 等待队列（FIFO）
     */
    private final    Queue<Waiter> waiters = new ConcurrentLinkedQueue<>();

    /**
     * 构造 MqttOutbox。
     *
     * @param maxInflight 最大 inflight 消息数，<=0 则无限制
     */
    public MqttOutbox(int maxInflight) {
        this.maxPermits = maxInflight <= 0 ? Integer.MAX_VALUE : maxInflight;
        this.permits = new AtomicInteger(maxInflight <= 0 ? Integer.MAX_VALUE : maxInflight);
    }

    /**
     * 获取一个出站许可（当 inflight 满时挂起等待）。
     *
     * @param packetId 请求的 packetId
     * @return 获取许可后完成的 Mono
     */
    public Mono<Void> acquire(int packetId) {
        return Mono.create(sink -> {
            // 尝试原子获取一个许可
            if (tryAcquirePermit()) {
                sink.success();
                return;
            }
            // 无可用许可 -> 入队等待，release 时被唤醒
            waiters.offer(new Waiter(packetId, sink));
        });
    }

    /**
     * 释放一个 slot（PUBACK/PUBCOMP 到达时）。唤醒一个等待者。
     *
     * @param packetId 已完成的 packetId
     */
    public void release(int packetId) {
        Waiter w = waiters.poll();
        if (w != null) {
            // 将释放的许可直接交给等待者（不归还许可池）
            w.sink().success();
        } else {
            // 无等待者 -> 归还许可
            incrementPermit();
        }
    }

    /**
     * 取消一个 pending 条目，不向等待者释放许可。
     *
     * @param packetId 要取消的 packetId
     */
    public void remove(int packetId) {
        // best-effort：waiters 按 packetId 标识但 poll 是 FIFO；取消路径下排空并重新入队非匹配项。
        int size = waiters.size();
        for (int i = 0; i < size; i++) {
            Waiter w = waiters.poll();
            if (w == null) {
                break;
            }
            if (w.packetId() == packetId) {
                w.sink().success();   // 让其继续；无害
            } else {
                waiters.offer(w);
            }
        }
    }

    /**
     * 当前可用许可数。
     *
     * @return 可用许可数量
     */
    public int availablePermits() {
        return Math.max(0, permits.get());
    }

    /**
     * 当前等待者数量。
     *
     * @return 等待者数量
     */
    public int waiting() {
        return waiters.size();
    }

    /**
     * 根据 CONNACK Receive Maximum 调整出站 inflight 上限（MQTT 5）。
     *
     * @param newMax 新的最大许可数
     */
    public void setMaxPermits(int newMax) {
        if (newMax <= 0) {
            return;
        }
        this.maxPermits = newMax;
    }

    private boolean tryAcquirePermit() {
        while (true) {
            int available = permits.get();
            if (available <= 0) {
                return false;
            }
            if (permits.compareAndSet(available, available - 1)) {
                return true;
            }
        }
    }

    private void incrementPermit() {
        while (true) {
            int current = permits.get();
            if (current >= maxPermits) {
                return; // 已达上限，不超出
            }
            if (permits.compareAndSet(current, current + 1)) {
                return;
            }
        }
    }

    private record Waiter(int packetId, MonoSink<Void> sink) {
    }

}
