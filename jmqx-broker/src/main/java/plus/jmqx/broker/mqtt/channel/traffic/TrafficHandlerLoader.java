package plus.jmqx.broker.mqtt.channel.traffic;

import io.netty.handler.traffic.AbstractTrafficShapingHandler;

/**
 * 尽量简洁一句描述
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
