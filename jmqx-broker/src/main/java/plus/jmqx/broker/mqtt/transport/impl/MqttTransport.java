package plus.jmqx.broker.mqtt.transport.impl;

import lombok.extern.slf4j.Slf4j;
import plus.jmqx.broker.mqtt.MqttConfiguration;
import plus.jmqx.broker.mqtt.context.ContextHolder;
import plus.jmqx.broker.mqtt.context.MqttReceiveContext;
import plus.jmqx.broker.mqtt.context.ReceiveContext;
import plus.jmqx.broker.mqtt.transport.Transport;
import plus.jmqx.broker.mqtt.transport.receiver.Receiver;
import plus.jmqx.broker.mqtt.transport.receiver.impl.MqttReceiver;
import plus.jmqx.broker.mqtt.transport.receiver.impl.MqttWsReceiver;
import plus.jmqx.broker.mqtt.transport.receiver.impl.MqttWssReceiver;
import plus.jmqx.broker.mqtt.transport.receiver.impl.MqttsReceiver;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;

import java.util.Optional;

/**
 * MQTT服务器协议实现
 *
 * @author maxid
 * @since 2025/4/8 17:31
 */
@Slf4j
public class MqttTransport implements Transport<MqttConfiguration> {

    private final Receiver          receiver;
    private final MqttConfiguration configuration;
    private       DisposableServer  disposableServer;

    public MqttTransport(Receiver receiver, MqttConfiguration configuration) {
        this.receiver = receiver;
        this.configuration = configuration;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> Optional.ofNullable(disposableServer).ifPresent(DisposableServer::dispose)));
    }

    /**
     * 启动 MQTT 服务
     *
     * @param config 配置信息
     * @return MQTT 传输服务
     */
    public static Transport<MqttConfiguration> startMqtt(MqttConfiguration config) {
        return new MqttTransport(new MqttReceiver(), config);
    }

    /**
     * 启动 MQTTS 服务
     *
     * @param config 配置信息
     * @return MQTTS 传输服务
     */
    public static Transport<MqttConfiguration> startMqtts(MqttConfiguration config) {
        return new MqttTransport(new MqttsReceiver(), config);
    }

    /**
     * 启动 MQTT WS 服务
     *
     * @param config 配置信息
     * @return MQTT WS 传输服务
     */
    public static Transport<MqttConfiguration> startMqttWs(MqttConfiguration config) {
        return new MqttTransport(new MqttWsReceiver(), config);
    }

    /**
     * 启动 MQTT WSS 服务
     *
     * @param config 配置信息
     * @return MQTT WSS 传输服务
     */
    public static Transport<MqttConfiguration> startMqttWss(MqttConfiguration config) {
        return new MqttTransport(new MqttWssReceiver(), config);
    }

    @Override
    public Mono<Transport> start() {
        return Mono.deferContextual(view -> receiver.bind())
                .doOnNext(this::setDisposableServer)
                .thenReturn(this)
                .doOnSuccess(transport -> log.info(""))
                .cast(Transport.class)
                .contextWrite(context -> context.put(MqttReceiveContext.class, this.context(configuration)));
    }

    @Override
    public ReceiveContext<MqttConfiguration> context(MqttConfiguration configuration) {
        synchronized (this) {
            if (ContextHolder.getContext() == null) {
                MqttReceiveContext context = new MqttReceiveContext(configuration, this);
                ContextHolder.setContext(context);
            }
            return (ReceiveContext<MqttConfiguration>) ContextHolder.getContext();
        }
    }

    @Override
    public void dispose() {
        if (disposableServer != null) {
            this.disposableServer.dispose();
        }
    }

    @Override
    public boolean isDisposed() {
        if (disposableServer == null) {
            return false;
        }
        return this.disposableServer.isDisposed();
    }

    private void setDisposableServer(DisposableServer disposableServer) {
        this.disposableServer = disposableServer;
    }
}
