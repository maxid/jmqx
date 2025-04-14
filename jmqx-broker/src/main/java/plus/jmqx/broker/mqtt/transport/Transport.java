package plus.jmqx.broker.mqtt.transport;

import plus.jmqx.broker.config.Configuration;
import plus.jmqx.broker.mqtt.context.ReceiveContext;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

/**
 * Netty 服务器协议实现接口定义
 *
 * @author maxid
 * @since 2025/4/8 17:38
 */
public interface Transport<C extends Configuration> extends Disposable {
    /**
     * 启动传输服务
     *
     * @return {@link Transport} 传输服务
     */
    Mono<Transport> start();

    /**
     * 初始化服务上下文
     *
     * @param configuration 配置信息
     * @return 服务上下文
     */
    ReceiveContext<C> context(C configuration);
}
