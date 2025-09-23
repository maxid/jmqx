package plus.jmqx.broker.mqtt.channel.traffic;

import io.netty.handler.traffic.AbstractTrafficShapingHandler;

/**
 * 流控
 *
 * @author maxid
 * @since 2025/4/10 17:25
 */
public interface TrafficHandlerLoader {
    /**
     * return TrafficHandlerLoader
     *
     * @return {@link AbstractTrafficShapingHandler}
     */
    AbstractTrafficShapingHandler get();
}
