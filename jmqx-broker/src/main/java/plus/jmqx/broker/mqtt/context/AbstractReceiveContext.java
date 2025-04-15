package plus.jmqx.broker.mqtt.context;

import io.netty.handler.traffic.GlobalChannelTrafficShapingHandler;
import io.netty.handler.traffic.GlobalTrafficShapingHandler;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import plus.jmqx.broker.config.Configuration;
import plus.jmqx.broker.mqtt.channel.traffic.TrafficHandlerLoader;
import plus.jmqx.broker.mqtt.channel.traffic.impl.CacheTrafficHandlerLoader;
import plus.jmqx.broker.mqtt.channel.traffic.impl.LazyTrafficHandlerLoader;
import plus.jmqx.broker.mqtt.message.MessageAdapter;
import plus.jmqx.broker.mqtt.message.MessageProcessor;
import plus.jmqx.broker.mqtt.message.impl.MqttMessageAdapter;
import plus.jmqx.broker.mqtt.retry.TimeAckManager;
import plus.jmqx.broker.mqtt.transport.Transport;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.netty.resources.LoopResources;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * 上下文抽象
 *
 * @author maxid
 * @since 2025/4/10 18:14
 */
@Slf4j
@Getter
@Setter
public abstract class AbstractReceiveContext<T extends Configuration> implements ReceiveContext<T> {
    /**
     * 配置信息
     */
    private final T                    configuration;
    /**
     * 传输服务
     */
    private final Transport<T>         transport;
    /**
     * NioEventLoop 配置
     */
    private final LoopResources        loopResources;
    /**
     * 流控配置
     */
    private final TrafficHandlerLoader trafficHandlerLoader;
    /**
     * ACK
     */
    private final TimeAckManager timeAckManager;
    /**
     * MQTT 消息处理适配器
     */
    private final MessageAdapter messageAdapter;

    public AbstractReceiveContext(T config, Transport<T> transport) {
        this.configuration = config;
        this.transport = transport;
        this.loopResources = LoopResources.create("jmqx-event-loop", config.getBossThreadSize(), config.getWorkThreadSize(), true);
        this.trafficHandlerLoader = trafficHandlerLoader();
        this.timeAckManager = new TimeAckManager(20, TimeUnit.MILLISECONDS, 50);
        this.messageAdapter = messageAdapter();
    }

    private TrafficHandlerLoader trafficHandlerLoader() {
        if (configuration.getGlobalReadWriteSize() == null && configuration.getChannelReadWriteSize() == null) {
            return new CacheTrafficHandlerLoader(new GlobalTrafficShapingHandler(this.loopResources.onServer(true).next(), 60 * 1000));
        } else if (configuration.getChannelReadWriteSize() == null) {
            String[] limits = configuration.getGlobalReadWriteSize().split(",");
            return new CacheTrafficHandlerLoader(new GlobalTrafficShapingHandler(this.loopResources.onServer(true), Long.parseLong(limits[1]), Long.parseLong(limits[0]), 60 * 1000));
        } else if (configuration.getGlobalReadWriteSize() == null) {
            String[] limits = configuration.getChannelReadWriteSize().split(",");
            return new LazyTrafficHandlerLoader(() -> new GlobalTrafficShapingHandler(this.loopResources.onServer(true), Long.parseLong(limits[1]), Long.parseLong(limits[0]), 60 * 1000));
        } else {
            String[] globalLimits = configuration.getGlobalReadWriteSize().split(",");
            String[] channelLimits = configuration.getChannelReadWriteSize().split(",");
            return new CacheTrafficHandlerLoader(new GlobalChannelTrafficShapingHandler(this.loopResources.onServer(true), Long.parseLong(globalLimits[1]), Long.parseLong(globalLimits[0]), Long.parseLong(channelLimits[1]), Long.parseLong(channelLimits[0]), 60 * 1000));
        }
    }

    private MessageAdapter messageAdapter() {
        Scheduler scheduler = Schedulers.newBoundedElastic(configuration.getBusinessThreadSize(), configuration.getBusinessQueueSize(), "business-io");
        return Optional.ofNullable(MessageAdapter.INSTANCE).orElse(new MqttMessageAdapter(scheduler));
    }
}
