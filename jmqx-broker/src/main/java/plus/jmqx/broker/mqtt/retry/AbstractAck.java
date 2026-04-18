package plus.jmqx.broker.mqtt.retry;

import io.netty.util.Timeout;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;

/**
 * ACK 确认机制抽象
 *
 * @author maxid
 * @since 2025/4/11 09:16
 */
@Slf4j(topic = "ack")
public abstract class AbstractAck implements Ack {

    /**
     * 最大重试次数
     */
    private final    int        maxRetrySize;
    private          int        count = 1;
    private volatile boolean    died  = false;
    private final    Runnable   runnable;
    private final    AckManager ackManager;
    private final    int        period;
    private final    Runnable   cleaner;

    /**
     * 构造 ACK 重试基础实现
     *
     * @param maxRetrySize 最大重试次数
     * @param period       重试周期
     * @param runnable     重试任务
     * @param ackManager   ACK 管理器
     * @param cleaner      清理任务
     */
    protected AbstractAck(int maxRetrySize, int period, Runnable runnable, AckManager ackManager, Runnable cleaner) {
        this.maxRetrySize = maxRetrySize;
        this.period = period;
        this.runnable = runnable;
        this.ackManager = ackManager;
        this.cleaner = cleaner;
    }

    /**
     * 执行重试任务
     *
     * @param timeout 超时对象
     * @throws Exception 执行异常
     */
    @Override
    public void run(Timeout timeout) throws Exception {
        if (++count <= maxRetrySize + 1 && !died) {
            try {
                log.debug("task retry send ...........");
                runnable.run();
                ackManager.addAck(this);
            } catch (Exception e) {
                log.error("Ack error ", e);
            }
        } else {
            cleaner.run();
        }
    }

    /**
     * 停止重试
     */
    @Override
    public void stop() {
        died = true;
        log.debug("retry task  stop ...........");
        ackManager.deleteAck(getId());
    }

    /**
     * 启动重试
     */
    @Override
    public void start() {
        this.ackManager.addAck(this);
    }

    /**
     * 获取当前重试间隔
     *
     * @return 间隔时长
     */
    @Override
    public int getTimed() {
        return this.period * this.count;
    }

    /**
     * 获取间隔时间单位
     *
     * @return 时间单位
     */
    @Override
    public TimeUnit getUnit() {
        return TimeUnit.SECONDS;
    }

}
