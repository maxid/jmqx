package plus.jmqx.broker.support;

/**
 * MQTT 消息压测结果
 */
public class StressResult {

    public final long sent;
    public final long startNanos;
    public final long endNanos;
    public final boolean completed;

    public StressResult(long sent, long startNanos, long endNanos, boolean completed) {
        this.sent = sent;
        this.startNanos = startNanos;
        this.endNanos = endNanos;
        this.completed = completed;
    }
}
