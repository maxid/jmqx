package plus.jmqx.broker.mqtt.transport.receiver;

import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;

/**
 * Netty 服务器接口
 *
 * @author maxid
 * @since 2025/4/8 17:59
 */
public interface Receiver {
    /**
     * 服务端口绑定
     *
     * @return {@link DisposableServer} 服务器
     */
    Mono<DisposableServer> bind();
}
