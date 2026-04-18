package plus.jmqx.broker.mqtt.channel.traffic.impl;

import io.netty.handler.traffic.AbstractTrafficShapingHandler;
import lombok.AllArgsConstructor;
import plus.jmqx.broker.mqtt.channel.traffic.TrafficHandlerLoader;

/**
 * 流控
 *
 * @author maxid
 * @since 2025/4/10 17:49
 */
@AllArgsConstructor
public class CacheTrafficHandlerLoader  implements TrafficHandlerLoader {

    private final AbstractTrafficShapingHandler trafficShapingHandler;

    /**
     * 获取缓存的流控处理器
     *
     * @return 流控处理器
     */
    @Override
    public AbstractTrafficShapingHandler get() {
        return this.trafficShapingHandler;
    }

}
