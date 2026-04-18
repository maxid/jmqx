package plus.jmqx.broker.mqtt.retry;

/**
 * 重试确认机制实现
 *
 * @author maxid
 * @since 2025/4/11 09:17
 */
public class RetryAck extends AbstractAck {

    private final long id;

    /**
     * 构造重试 ACK
     *
     * @param id           ACK ID
     * @param maxRetrySize 最大重试次数
     * @param period       重试周期
     * @param runnable     重试任务
     * @param ackManager   ACK 管理器
     * @param cleaner      清理任务
     */
    public RetryAck(long id, int maxRetrySize, int period, Runnable runnable, AckManager ackManager, Runnable cleaner) {
        super(maxRetrySize, period, runnable, ackManager, cleaner);
        this.id = id;
    }

    /**
     * 获取 ACK ID
     *
     * @return ACK ID
     */
    @Override
    public long getId() {
        return this.id;
    }

}
