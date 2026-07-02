package plus.jmqx.client.mqtt.internal.buffer;

import lombok.extern.slf4j.Slf4j;
import plus.jmqx.client.mqtt.internal.MqttPublishResultImpl;
import plus.jmqx.client.mqtt.message.MqttPublish;
import plus.jmqx.client.mqtt.message.MqttPublishResult;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * 离线消息缓冲。
 *
 * <p>{@link #offer} 返回一个 pending {@code Mono<MqttPublishResult>}，当 {@link #flush} 将消息通过 writer
 * 重新回放（运行完整 publish() 路径，重新注册 AckTracker）产出最终结果时完成。
 * 修复了旧设计中 "flush 绕过 AckTracker" 的缺陷。
 *
 * @author maxid
 */
@Slf4j
public final class MessageBuffer {

    /**
     * 缓存的消息队列
     */
    private final Queue<BufferedPublish> queue        = new ConcurrentLinkedQueue<>();
    /**
     * 最大消息条数限制
     */
    private final int                    maxSize;
    /**
     * 最大字节数限制
     */
    private final long                   maxBytes;
    /**
     * 当前已缓存字节数
     */
    private final AtomicLong             currentBytes = new AtomicLong(0);

    /**
     * 构造 MessageBuffer。
     *
     * @param maxSize  最大消息条数，<=0 则无限制
     * @param maxBytes 最大字节数，<=0 则无限制
     */
    public MessageBuffer(int maxSize, long maxBytes) {
        this.maxSize = maxSize <= 0 ? Integer.MAX_VALUE : maxSize;
        this.maxBytes = maxBytes <= 0 ? Long.MAX_VALUE : maxBytes;
    }

    /**
     * 将消息放入离线缓冲。
     *
     * @param publish 发布消息
     * @return 待完成的发布结果 Mono（flush 时完成）
     */
    public Mono<MqttPublishResult> offer(MqttPublish publish) {
        long bytes = estimateBytes(publish);
        if (currentBytes.get() + bytes > maxBytes || queue.size() >= maxSize) {
            return Mono.error(new MessageBufferFullException());
        }
        Sinks.One<MqttPublishResult> sink = Sinks.one();
        queue.offer(new BufferedPublish(publish, sink));
        currentBytes.addAndGet(bytes);
        return sink.asMono();
    }

    /**
     * 通过 writer（完整 publish 路径）回放缓存的消息。返回 {@code Mono<Void>}，所有消息回放完成时完成。
     *
     * @param writer 用于实际发送消息的函数
     * @return 所有消息回放完成后完成的 Mono
     */
    public Mono<Void> flush(Function<MqttPublish, Mono<MqttPublishResult>> writer) {
        return Mono.defer(() -> {
            List<Mono<Void>> sends = new ArrayList<>();
            BufferedPublish bp;
            while ((bp = queue.poll()) != null) {
                currentBytes.addAndGet(-estimateBytes(bp.publish()));
                BufferedPublish captured = bp;
                sends.add(writer.apply(captured.publish())
                        .doOnNext(r -> captured.sink().tryEmitValue(r))
                        .doOnError(e -> captured.sink().tryEmitError(e))
                        .then());
            }
            return Flux.concat(sends).then();
        });
    }

    /**
     * 失败所有缓存消息（禁用重连的断开时调用）。
     *
     * @param error 失败原因
     */
    public void failAll(Throwable error) {
        BufferedPublish bp;
        while ((bp = queue.poll()) != null) {
            currentBytes.addAndGet(-estimateBytes(bp.publish()));
            bp.sink().tryEmitValue(new MqttPublishResultImpl(bp.publish(), error));
        }
    }

    /**
     * 清空缓存
     */
    public void clear() {
        queue.clear();
        currentBytes.set(0);
    }

    /**
     * 当前缓存的消息数量。
     *
     * @return 消息条数
     */
    public int size() {
        return queue.size();
    }

    /**
     * 当前缓存的字节数。
     *
     * @return 字节数
     */
    public long bytes() {
        return currentBytes.get();
    }

    private long estimateBytes(MqttPublish p) {
        return (p.getTopic() != null ? p.getTopic().length() : 0)
                + (p.getPayloadAsBytes() != null ? p.getPayloadAsBytes().length : 0);
    }

    private record BufferedPublish(MqttPublish publish, Sinks.One<MqttPublishResult> sink) {
    }

}
