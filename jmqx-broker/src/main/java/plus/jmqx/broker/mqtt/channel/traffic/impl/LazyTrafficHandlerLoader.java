package plus.jmqx.broker.mqtt.channel.traffic.impl;

import io.netty.handler.traffic.AbstractTrafficShapingHandler;
import lombok.AllArgsConstructor;
import plus.jmqx.broker.mqtt.channel.traffic.TrafficHandlerLoader;

import java.util.function.Supplier;

/**
 * 懒流控
 *
 * @author maxid
 * @since 2025/4/10 17:50
 */
@AllArgsConstructor
public class LazyTrafficHandlerLoader implements TrafficHandlerLoader {

    private final Supplier<AbstractTrafficShapingHandler> shapingHandlerSupplier;

    /**
     * 获取流控处理器实例。
     *
     * @return 流控处理器
     */
    @Override
    public AbstractTrafficShapingHandler get() {
        return this.shapingHandlerSupplier.get();
    }

}
