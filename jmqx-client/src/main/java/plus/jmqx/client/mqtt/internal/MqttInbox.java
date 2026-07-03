package plus.jmqx.client.mqtt.internal;

import lombok.extern.slf4j.Slf4j;
import plus.jmqx.client.mqtt.message.MqttPublish;
import plus.jmqx.client.mqtt.message.QoS;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 入站投递枢纽。
 *
 * <p>每个投递的 PUBLISH 都与其 ack {@link Runnable} 一起包装；下游 Flux 仅在 {@code request(n)} 后向订阅者发射，
 * 订阅者随后调用 {@code ack()}。这使得 MQTT 的 ACK 成为天然背压信号：
 * 慢订阅者延迟 ACK → broker 暂停推送（QoS1/2）。
 *
 * @author maxid
 */
@Slf4j
public final class MqttInbox {

    /**
     * 入站消息发射器
     */
    private final Sinks.Many<Deliverable> sink;
    /**
     * 缓冲容量上限
     */
    private final int                     bufferSize;
    /**
     * 当前待消费消息数
     */
    private final AtomicInteger           pending = new AtomicInteger(0);

    /**
     * 构造 MqttInbox。
     *
     * @param bufferSize 缓冲大小，&lt;=0 则使用 Integer.MAX_VALUE
     */
    public MqttInbox(int bufferSize) {
        this.bufferSize = bufferSize <= 0 ? Integer.MAX_VALUE : bufferSize;
        this.sink = Sinks.many().multicast().onBackpressureBuffer(this.bufferSize, false);
    }

    /**
     * 投递一个 publish 及其 ack 动作。缓冲满时返回 false（调用方应对 QoS0 丢弃）。
     *
     * @param pub       发布消息
     * @param ackAction ack 回调动作
     * @return 投递成功返回 true，缓冲满返回 false
     */
    public boolean deliver(MqttPublish pub, Runnable ackAction) {
        // 显式容量门控：multicast sink 在无订阅者时不自动溢出失败，故以计数器强制。
        while (true) {
            int current = pending.get();
            if (current >= bufferSize) {
                log.warn("Inbox buffer full, dropping QoS{} publish on {}", pub.getQoS(), pub.getTopic());
                return false;
            }
            if (pending.compareAndSet(current, current + 1)) {
                break;
            }
        }
        Deliverable d = new Deliverable(pub, ackAction, pending::decrementAndGet);
        Sinks.EmitResult r = sink.tryEmitNext(d);
        if (r.isFailure()) {
            pending.decrementAndGet();
            log.warn("Inbox emit failed ({}), dropping QoS{} publish on {}", r, pub.getQoS(), pub.getTopic());
            return false;
        }
        return true;
    }

    /**
     * 全局入站流（用于 publishes(ALL/SUBSCRIBED/UNSOLICITED)）。返回 Deliverable 以便 ack() 可达。
     *
     * @return 入站 Deliverable 流
     */
    public Flux<Deliverable> globalFlux() {
        return sink.asFlux();
    }

    /**
     * 订阅专属流。
     *
     * @return 订阅者专属的入站流
     */
    public Flux<Deliverable> subscriptionFlux() {
        return globalFlux();
    }

    /**
     * 包装器，携带 ack 动作，使订阅者可在消费时触发。
     *
     * <p>实现 {@link MqttPublish}，这样它本身就是一个可被订阅者消费的 publish（带可用的 ack）。
     * 每个元素在被下游消费时（onNext 后）通过 onConsume 计数递减以维护缓冲容量。
     */
    public static final class Deliverable implements MqttPublish {

        /**
         * 代理的真实发布消息
         */
        private final MqttPublish   delegate;
        /**
         * ack 回调动作
         */
        private final Runnable      ack;
        /**
         * 消费后释放容量的回调
         */
        private final Runnable      onConsume;
        /**
         * 是否已 ACK
         */
        private final AtomicBoolean acked    = new AtomicBoolean();
        /**
         * 是否已消费
         */
        private final AtomicBoolean consumed = new AtomicBoolean();

        /**
         * 构造 Deliverable。
         *
         * @param delegate  代理的真实发布消息
         * @param ack       ack 回调
         * @param onConsume 消费后回调
         */
        Deliverable(MqttPublish delegate, Runnable ack, Runnable onConsume) {
            this.delegate = delegate;
            this.ack = ack;
            this.onConsume = onConsume;
        }

        /**
         * 在下游消费后调用一次以释放缓冲容量。由引擎的 toMqtt3/toMqtt5 适配器在 doOnNext 中触发。
         */
        public void consume() {
            if (consumed.compareAndSet(false, true)) {
                onConsume.run();
            }
        }

        @Override
        public void ack() {
            if (acked.compareAndSet(false, true)) {
                ack.run();
            }
        }

        @Override
        public String getTopic() {
            return delegate.getTopic();
        }

        @Override
        public byte[] getPayloadAsBytes() {
            return delegate.getPayloadAsBytes();
        }

        @Override
        public QoS getQoS() {
            return delegate.getQoS();
        }

        @Override
        public boolean isRetain() {
            return delegate.isRetain();
        }

        @Override
        public boolean isDup() {
            return delegate.isDup();
        }

        @Override
        public int getPacketId() {
            return delegate.getPacketId();
        }
    }

}
