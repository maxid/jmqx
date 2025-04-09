package plus.jmqx.broker.mqtt.transport;

import plus.jmqx.broker.config.Configuration;
import reactor.core.Disposable;

/**
 * Netty 服务器协议实现接口定义
 *
 * @author maxid
 * @since 2025/4/8 17:38
 */
public interface Transport<C extends Configuration> extends Disposable {

}
