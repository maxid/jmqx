package plus.jmqx.broker.mqtt.retry;

import io.netty.util.TimerTask;

import java.util.concurrent.TimeUnit;

/**
 * ACK 确认机制定义
 *
 * @author maxid
 * @since 2025/4/11 09:11
 */
public interface Ack extends TimerTask {
    /**
     * ACK ID
     *
     * @return ACK ID
     */
    long getId();

    /**
     * 启动确认
     */
    void start();

    /**
     * 停止确认
     */
    void stop();

    /**
     * @return
     */
    int getTimed();

    /**
     * 时间调度器间隔时间单位
     *
     * @return 时间单位
     */
    TimeUnit getUnit();
}
