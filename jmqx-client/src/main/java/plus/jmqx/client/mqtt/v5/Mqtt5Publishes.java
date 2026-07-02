package plus.jmqx.client.mqtt.v5;

import plus.jmqx.client.mqtt.v5.message.Mqtt5Publish;

import java.time.Duration;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 阻塞式入站 publish 接收句柄（MQTT 5）。
 *
 * @author maxid
 */
public interface Mqtt5Publishes extends AutoCloseable, Iterable<Mqtt5Publish> {

    Mqtt5Publish receive() throws InterruptedException;

    Optional<Mqtt5Publish> receive(Duration timeout);

    default Optional<Mqtt5Publish> receiveNow() {
        return receive(Duration.ZERO);
    }

    @Override
    void close();

    @Override
    default Iterator<Mqtt5Publish> iterator() {
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                return true;
            }

            @Override
            public Mqtt5Publish next() {
                try {
                    return receive();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
        };
    }

    static Mqtt5Publishes fromQueue(BlockingQueue<Mqtt5Publish> queue, Runnable onClose) {
        AtomicBoolean closed = new AtomicBoolean();
        return new Mqtt5Publishes() {
            @Override
            public Mqtt5Publish receive() throws InterruptedException {
                return queue.take();
            }

            @Override
            public Optional<Mqtt5Publish> receive(Duration timeout) {
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
