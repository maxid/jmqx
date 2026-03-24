package plus.jmqx.broker.mqtt.retry;

import io.netty.util.HashedWheelTimer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * ACK 时间调度管理器
 *
 * @author maxid
 * @since 2025/4/10 18:45
 */
public class TimeAckManager extends HashedWheelTimer implements AckManager {

    private final Map<Long, Ack> acks = new ConcurrentHashMap<>();

    /**
     * 构造 ACK 时间调度管理器。
     *
     * @param tick  刻度间隔
     * @param unit  时间单位
     * @param wheel 轮盘大小
     */
    public TimeAckManager(long tick, TimeUnit unit, int wheel) {
        super(tick, unit, wheel);
    }

    /**
     * 添加 ACK 并调度超时任务。
     *
     * @param ack ACK 实例
     */
    @Override
    public void addAck(Ack ack) {
        this.acks.put(ack.getId(), ack);
        this.newTimeout(ack, ack.getTimed(), ack.getUnit());
    }

    /**
     * 获取 ACK。
     *
     * @param id ACK ID
     * @return ACK 实例
     */
    @Override
    public Ack getAck(Long id) {
        return this.acks.get(id);
    }

    /**
     * 删除 ACK。
     *
     * @param id ACK ID
     */
    @Override
    public void deleteAck(Long id) {
        this.acks.remove(id);
    }
}
