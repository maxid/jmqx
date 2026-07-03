package plus.jmqx.client.mqtt.stress;

import plus.jmqx.client.mqtt.message.MqttPublishResult;
import plus.jmqx.client.mqtt.message.QoS;
import plus.jmqx.client.mqtt.v3.Mqtt3AsyncClient;
import plus.jmqx.client.mqtt.v3.message.Mqtt3Publish;
import plus.jmqx.client.mqtt.v5.Mqtt5AsyncClient;
import plus.jmqx.client.mqtt.v5.message.Mqtt5Publish;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 按 inflight 窗口发送并等待每条 PUBLISH 完成（QoS1/2 即 broker PUBACK/PUBCOMP 确认）。
 */
public final class AckAwareStressPublisher {

    private AckAwareStressPublisher() {
    }

    public record PublishStats(long target, long acked, long failed) {
    }

    public static final class PublishProgress {
        public final AtomicLong sent   = new AtomicLong();
        public final AtomicLong acked  = new AtomicLong();
        public final AtomicLong failed = new AtomicLong();
    }

    public static PublishStats publishV3(Mqtt3AsyncClient client, String topic, byte[] payload, QoS qos,
                                         long totalMessages, int inflightWindow, long timeoutSeconds)
            throws Exception {
        return publishV3(client, topic, payload, qos, totalMessages, inflightWindow, timeoutSeconds, null);
    }

    public static PublishStats publishV3(Mqtt3AsyncClient client, String topic, byte[] payload, QoS qos,
                                         long totalMessages, int inflightWindow, long timeoutSeconds,
                                         PublishProgress progress) throws Exception {
        return publish(totalMessages, inflightWindow, timeoutSeconds, progress,
                () -> client.publish(Mqtt3Publish.builder().topic(topic).payload(payload).qos(qos).build()));
    }

    public static PublishStats publishV5(Mqtt5AsyncClient client, String topic, byte[] payload, QoS qos,
                                         long totalMessages, int inflightWindow, long timeoutSeconds)
            throws Exception {
        return publishV5(client, topic, payload, qos, totalMessages, inflightWindow, timeoutSeconds, null);
    }

    public static PublishStats publishV5(Mqtt5AsyncClient client, String topic, byte[] payload, QoS qos,
                                         long totalMessages, int inflightWindow, long timeoutSeconds,
                                         PublishProgress progress) throws Exception {
        return publish(totalMessages, inflightWindow, timeoutSeconds, progress,
                () -> client.publish(Mqtt5Publish.builder().topic(topic).payload(payload).qos(qos).build()));
    }

    @FunctionalInterface
    public interface PublishCall {
        CompletableFuture<? extends MqttPublishResult> publish();
    }

    private static PublishStats publish(long totalMessages, int inflightWindow, long timeoutSeconds,
                                        PublishProgress progress, PublishCall publishCall) throws Exception {
        if (totalMessages <= 0) {
            return new PublishStats(0, 0, 0);
        }
        int window = Math.max(1, inflightWindow);
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
        long sent = 0;
        long acked = 0;
        long failed = 0;
        List<CompletableFuture<? extends MqttPublishResult>> inflight = new ArrayList<>(window);

        while (sent < totalMessages || !inflight.isEmpty()) {
            while (sent < totalMessages && inflight.size() < window) {
                inflight.add(publishCall.publish());
                sent++;
                if (progress != null) {
                    progress.sent.incrementAndGet();
                }
            }
            if (inflight.isEmpty()) {
                break;
            }
            long remainingMs = Math.max(1, TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime()));
            CompletableFuture<Object> any = CompletableFuture.anyOf(
                    inflight.toArray(new CompletableFuture[0]));
            try {
                any.get(remainingMs, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                break;
            }
            Iterator<CompletableFuture<? extends MqttPublishResult>> it = inflight.iterator();
            while (it.hasNext()) {
                CompletableFuture<? extends MqttPublishResult> future = it.next();
                if (!future.isDone()) {
                    continue;
                }
                it.remove();
                try {
                    MqttPublishResult result = future.getNow(null);
                    if (result != null && result.getError() == null) {
                        acked++;
                        if (progress != null) {
                            progress.acked.incrementAndGet();
                        }
                    } else {
                        failed++;
                        if (progress != null) {
                            progress.failed.incrementAndGet();
                        }
                    }
                } catch (Exception ex) {
                    failed++;
                    if (progress != null) {
                        progress.failed.incrementAndGet();
                    }
                }
            }
        }
        for (CompletableFuture<? extends MqttPublishResult> future : inflight) {
            if (!future.isDone()) {
                future.cancel(true);
                failed++;
                if (progress != null) {
                    progress.failed.incrementAndGet();
                }
            }
        }
        return new PublishStats(totalMessages, acked, failed);
    }

}
