package plus.jmqx.broker;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.LoggerFactory;
import plus.jmqx.broker.mqtt.MqttConfiguration;
import plus.jmqx.broker.mqtt.transport.Transport;
import plus.jmqx.broker.mqtt.transport.impl.MqttTransport;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.util.ArrayList;
import java.util.List;

/**
 * 服务入口
 *
 * @author maxid
 * @since 2025/4/8 17:32
 */
@Slf4j
@ToString
public class Bootstrap {

    private static final Sinks.One<Void>        START_ONLY_MQTT = Sinks.one();
    private final            List<Transport<?>> transports      = new ArrayList<>();

    /**
     * 启动服务
     * @return 服务
     */
    public Mono<Bootstrap> start() {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        loggerContext.getLogger("root").setLevel(ch.qos.logback.classic.Level.DEBUG);
        loggerContext.getLogger("reactor.netty").setLevel(ch.qos.logback.classic.Level.DEBUG);
        MqttConfiguration config = new MqttConfiguration();
        return MqttTransport.startMqtt(config)
                .start()
                .doOnError(err -> log.error("start mqtt error", err))
                .doOnSuccess(transports::add)
                .then(startMqtts(config))
                .then(startWs(config))
                .then(startWss(config))
                .thenReturn(this)
                .doOnSuccess(bootstrap -> {});

    }

    public void startAwait() {
        this.start().doOnError(err -> {
            log.error("bootstrap server start error", err);
            START_ONLY_MQTT.tryEmitEmpty();
        }).subscribe();
        START_ONLY_MQTT.asMono().block();
    }

    public void shutdown() {
        transports.forEach(Transport::dispose);
    }

    private Mono<Void> startMqtts(MqttConfiguration config ) {
        if(config.getSslEnable()) {
            return MqttTransport.startMqtts(config)
                    .start()
                    .doOnSuccess(transports::add)
                    .doOnError(err -> log.error("start mqtts error", err))
                    .then();
        }
        return Mono.empty();
    }

    private Mono<Void> startWs(MqttConfiguration config) {
        return MqttTransport.startMqttWs(config)
                .start()
                .doOnSuccess(transports::add)
                .doOnError(err -> log.error("start mqtt ws error", err))
                .then();
    }

    private Mono<Void> startWss(MqttConfiguration config ) {
        if(config.getSslEnable()) {
            return MqttTransport.startMqttWss(config)
                    .start()
                    .doOnSuccess(transports::add)
                    .doOnError(err -> log.error("start mqtt wss error", err))
                    .then();
        }
        return Mono.empty();
    }
}
