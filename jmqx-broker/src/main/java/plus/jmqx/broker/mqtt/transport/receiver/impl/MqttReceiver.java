package plus.jmqx.broker.mqtt.transport.receiver.impl;

import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelOption;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import plus.jmqx.broker.mqtt.MqttConfiguration;
import plus.jmqx.broker.mqtt.channel.MqttChannel;
import plus.jmqx.broker.mqtt.context.MqttReceiveContext;
import plus.jmqx.broker.mqtt.transport.handler.OptionHandler;
import plus.jmqx.broker.mqtt.transport.receiver.Receiver;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.tcp.TcpServer;
import reactor.util.context.ContextView;

/**
 * MQTT 服务器，MQTT(1883)
 *
 * @author maxid
 * @since 2025/4/9 11:47
 */
public class MqttReceiver extends OptionHandler implements Receiver {
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
        return server.port(config.getPort())
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
                    context.apply(MqttChannel.init(connection, context.getTimeAckManager()));
                });
    }
}
