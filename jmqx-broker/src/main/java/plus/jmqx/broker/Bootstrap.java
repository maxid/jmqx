package plus.jmqx.broker;

import lombok.AllArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import plus.jmqx.broker.acl.AclManager;
import plus.jmqx.broker.auth.AuthManager;
import plus.jmqx.broker.mqtt.MqttConfiguration;
import plus.jmqx.broker.mqtt.context.ContextHolder;
import plus.jmqx.broker.mqtt.message.dispatch.PlatformDispatcher;
import plus.jmqx.broker.mqtt.transport.Transport;
import plus.jmqx.broker.mqtt.transport.impl.MqttTransport;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

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
@AllArgsConstructor
public class Bootstrap {

    private static final Sinks.One<Void>    START_ONLY_MQTT = Sinks.one();
    private final        List<Transport<?>> transports      = new ArrayList<>();
    private final        MqttConfiguration  config;
    private final        AclManager         aclManager;
    private final        AuthManager        authManager;
    private final        PlatformDispatcher platformDispatcher;

    public Bootstrap(MqttConfiguration config) {
        this(config, null, null, null);
    }

    public Bootstrap(MqttConfiguration config, AclManager aclManager) {
        this(config, aclManager, null, null);
    }

    public Bootstrap(MqttConfiguration config, AuthManager authManager) {
        this(config, null, authManager, null);
    }

    public Bootstrap(MqttConfiguration config, PlatformDispatcher platformDispatcher) {
        this(config, null, null, platformDispatcher);
    }

    public Bootstrap(MqttConfiguration config, AclManager aclManager, AuthManager authManager) {
        this(config, aclManager, authManager, null);
    }

    /**
     * 启动服务
     *
     * @return 服务
     */
    public Mono<Bootstrap> start() {
        ContextHolder.setAclManager(aclManager);
        ContextHolder.setAuthManager(authManager);
        ContextHolder.setDispatchScheduler(Schedulers.newBoundedElastic(
                config.getBusinessThreadSize(),
                config.getBusinessQueueSize(),
                "jmqx-dispatch-io"
        ));
        ContextHolder.setPlatformDispatcher(platformDispatcher);
        return startMqtt(config)
                .then(startMqtts(config))
                .then(startWs(config))
                .then(startWss(config))
                .thenReturn(this)
                .doOnSuccess(bootstrap -> {
                });

    }

    /**
     * 启动服务，并阻塞
     */
    public void startAwait() {
        this.start().doOnError(err -> {
            log.error("bootstrap server start error", err);
            START_ONLY_MQTT.tryEmitEmpty();
        }).subscribe();
        START_ONLY_MQTT.asMono().block();
    }

    /**
     * 关闭服务
     */
    public void shutdown() {
        transports.forEach(Transport::dispose);
    }

    private Mono<Void> startMqtt(MqttConfiguration config) {
        if (config.getPort() > 0) {
            return MqttTransport.startMqtt(config)
                    .start()
                    .doOnSuccess(transports::add)
                    .doOnError(err -> log.error("start mqtt error", err))
                    .then();
        }
        return Mono.empty();
    }

    private Mono<Void> startMqtts(MqttConfiguration config) {
        if (config.getSslEnable() && config.getSecurePort() > 0) {
            return MqttTransport.startMqtts(config)
                    .start()
                    .doOnSuccess(transports::add)
                    .doOnError(err -> log.error("start mqtts error", err))
                    .then();
        }
        return Mono.empty();
    }

    private Mono<Void> startWs(MqttConfiguration config) {
        if (config.getWebsocketPort() > 0) {
            return MqttTransport.startMqttWs(config)
                    .start()
                    .doOnSuccess(transports::add)
                    .doOnError(err -> log.error("start mqtt ws error", err))
                    .then();
        }
        return Mono.empty();
    }

    private Mono<Void> startWss(MqttConfiguration config) {
        if (config.getSslEnable() && config.getWebsocketSecurePort() > 0) {
            return MqttTransport.startMqttWss(config)
                    .start()
                    .doOnSuccess(transports::add)
                    .doOnError(err -> log.error("start mqtt wss error", err))
                    .then();
        }
        return Mono.empty();
    }
}
