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

    private final Queue<BufferedPublish> queue        = new ConcurrentLinkedQueue<>();
    private final int                    maxSize;
    private final long                   maxBytes;
    private final AtomicLong             currentBytes = new AtomicLong(0);

    public MessageBuffer(int maxSize, long maxBytes) {
        this.maxSize = maxSize <= 0 ? Integer.MAX_VALUE : maxSize;
        this.maxBytes = maxBytes <= 0 ? Long.MAX_VALUE : maxBytes;
    }

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
     */
    public void failAll(Throwable error) {
        BufferedPublish bp;
        while ((bp = queue.poll()) != null) {
            currentBytes.addAndGet(-estimateBytes(bp.publish()));
            bp.sink().tryEmitValue(new MqttPublishResultImpl(bp.publish(), error));
        }
    }

    public void clear() {
        queue.clear();
        currentBytes.set(0);
    }

    public int size() {
        return queue.size();
    }

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
