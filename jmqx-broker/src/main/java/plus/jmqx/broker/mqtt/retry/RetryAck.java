package plus.jmqx.broker.mqtt.retry;

/**
 * 重试确认机制实现
 *
 * @author maxid
 * @since 2025/4/11 09:17
 */
public class RetryAck extends AbstractAck {

    private final long id;

    public RetryAck(long id, int maxRetrySize, int period, Runnable runnable, AckManager ackManager, Runnable cleaner) {
        super(maxRetrySize, period, runnable, ackManager, cleaner);
        this.id = id;
    }

    @Override
    public long getId() {
        return this.id;
    }
}
