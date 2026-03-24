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
     * 获取流控处理器。
     *
     * @return 流控处理器
     */
    AbstractTrafficShapingHandler get();

}
