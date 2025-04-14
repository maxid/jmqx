package plus.jmqx.broker.mqtt.transport.receiver.impl;

import plus.jmqx.broker.mqtt.transport.receiver.Receiver;
import plus.jmqx.broker.mqtt.handler.SslHandler;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.tcp.TcpServer;
import reactor.util.context.ContextView;

/**
 * MQTT Websocket Secure 服务器, WSS(8884)
 *
 * @author maxid
 * @since 2025/4/9 15:00
 */
public class MqttWssReceiver extends SslHandler implements Receiver {
    @Override
    public Mono<DisposableServer> bind() {
        return null;
    }

    private TcpServer serv(ContextView view) {
        return null;
    }
}
