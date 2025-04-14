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

    @Override
    public AbstractTrafficShapingHandler get() {
        return this.trafficShapingHandler;
    }
}
