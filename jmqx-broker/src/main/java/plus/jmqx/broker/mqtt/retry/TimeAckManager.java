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

    public TimeAckManager(long tick, TimeUnit unit, int wheel) {
        super(tick, unit, wheel);
    }

    @Override
    public void addAck(Ack ack) {
        this.acks.put(ack.getId(), ack);
        this.newTimeout(ack, ack.getTimed(), ack.getUnit());
    }

    @Override
    public Ack getAck(Long id) {
        return this.acks.get(id);
    }

    @Override
    public void deleteAck(Long id) {
        this.acks.remove(id);
    }
}
