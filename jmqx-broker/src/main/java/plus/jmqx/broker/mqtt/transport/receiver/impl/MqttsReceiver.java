package plus.jmqx.broker.mqtt.transport.receiver.impl;

import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelOption;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import plus.jmqx.broker.mqtt.MqttConfiguration;
import plus.jmqx.broker.mqtt.channel.MqttSession;
import plus.jmqx.broker.mqtt.context.MqttReceiveContext;
import plus.jmqx.broker.mqtt.transport.receiver.Receiver;
import plus.jmqx.broker.mqtt.transport.handler.SslHandler;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.tcp.TcpServer;
import reactor.util.context.ContextView;

/**
 * MQTT Secure 服务器，MQTTS(1884)
 *
 * @author maxid
 * @since 2025/4/9 15:00
 */
public class MqttsReceiver extends SslHandler implements Receiver {

    @Override
    public String getName() {
        return "mqtts";
    }

    @Override
    public Mono<DisposableServer> bind() {
        return Mono.deferContextual(view -> Mono.just(this.serv(view))
                .flatMap(serv-> serv.bind().cast(DisposableServer.class)));
    }

    private TcpServer serv(ContextView view) {
        MqttReceiveContext context = view.get(MqttReceiveContext.class);
        MqttConfiguration config = context.getConfiguration();
        WriteBufferWaterMark waterMark = new WriteBufferWaterMark(config.getLowWaterMark(), config.getHighWaterMark());
        TcpServer server = initTcpServer(config);
        return server.port(config.getSecurePort())
                .wiretap(config.getWiretap())
                .childOption(ChannelOption.WRITE_BUFFER_WATER_MARK, waterMark)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.SO_REUSEADDR, true)
                .metrics(false)
                .option(ChannelOption.SO_REUSEADDR, true)
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .runOn(context.getLoopResources())
                .doOnConnection(connection -> {
                    connection.addHandlerFirst(MqttEncoder.INSTANCE)
                            .addHandlerFirst(new MqttDecoder(config.getMessageMaxSize()))
                            .addHandlerFirst(context.getTrafficHandlerLoader().get());
                    context.apply(MqttSession.init(connection, context.getTimeAckManager()));
                });
    }
}
