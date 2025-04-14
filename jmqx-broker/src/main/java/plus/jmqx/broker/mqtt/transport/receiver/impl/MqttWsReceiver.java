package plus.jmqx.broker.mqtt.transport.receiver.impl;

import plus.jmqx.broker.mqtt.transport.receiver.Receiver;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.tcp.TcpServer;
import reactor.util.context.ContextView;

/**
 * MQTT Websocket 服务器, WS(8883)
 *
 * @author maxid
 * @since 2025/4/9 11:47
 */
public class MqttWsReceiver implements Receiver {
    @Override
    public Mono<DisposableServer> bind() {
        return null;
    }

    private TcpServer serv(ContextView view) {
        return null;
    }
}
