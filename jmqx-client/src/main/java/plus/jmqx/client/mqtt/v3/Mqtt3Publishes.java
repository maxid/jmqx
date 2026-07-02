package plus.jmqx.client.mqtt.v3;

import plus.jmqx.client.mqtt.v3.message.Mqtt3Publish;

import java.time.Duration;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 阻塞式入站 publish 接收句柄。
 *
 * <p>内部以 {@link BlockingQueue} 缓冲入站消息，{@link #receive()} 阻塞等待下一条。
 *
 * @author maxid
 * @since 1.4.14
 */
public interface Mqtt3Publishes extends AutoCloseable, Iterable<Mqtt3Publish> {

    /**
     * 阻塞接收下一条 publish。
     *
     * @return 下一条入站 publish
     * @throws InterruptedException 等待被中断
     */
    Mqtt3Publish receive() throws InterruptedException;

    /**
     * 阻塞接收下一条 publish，超时返回空。
     *
     * @param timeout 最大等待时间
     * @return 入站 publish，超时则为空
     */
    Optional<Mqtt3Publish> receive(Duration timeout);

    /**
     * 立即返回已排队的 publish，无则空。
     *
     * @return 已排队的 publish，无则空
     */
    default Optional<Mqtt3Publish> receiveNow() {
        return receive(Duration.ZERO);
    }

    /**
     * 关闭句柄并释放底层订阅资源。
     */
    @Override
    void close();

    @Override
    default Iterator<Mqtt3Publish> iterator() {
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                return true;
            }

            @Override
            public Mqtt3Publish next() {
                try {
                    return receive();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
        };
    }

    /**
     * 基于 {@link LinkedBlockingQueue} 的默认实现。
     *
     * @param queue   入站消息队列
     * @param onClose 关闭时执行的清理回调
     * @return publish 接收句柄
     */
    static Mqtt3Publishes fromQueue(BlockingQueue<Mqtt3Publish> queue, Runnable onClose) {
        AtomicBoolean closed = new AtomicBoolean();
        return new Mqtt3Publishes() {
            @Override
            public Mqtt3Publish receive() throws InterruptedException {
                return queue.take();
            }

            @Override
            public Optional<Mqtt3Publish> receive(Duration timeout) {
                try {
                    var p = queue.poll(timeout.toNanos(), TimeUnit.NANOSECONDS);
                    return Optional.ofNullable(p);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return Optional.empty();
                }
            }

            @Override
            public void close() {
                if (closed.compareAndSet(false, true)) {
                    onClose.run();
                }
            }
        };
    }

}
