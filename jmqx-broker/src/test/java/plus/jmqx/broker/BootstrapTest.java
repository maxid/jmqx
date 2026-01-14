package plus.jmqx.broker;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import plus.jmqx.broker.mqtt.MqttConfiguration;
import plus.jmqx.broker.mqtt.message.dispatch.*;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * MQTT Broker 测试用例
 *
 * @author maxid
 * @since 2025/4/22 10:46
 */
@Slf4j
class BootstrapTest {

    @Test
    void brokerTest() throws Exception {
        setLogContext();

        MqttConfiguration config1 = config("n1", 1883, 1884, 8883, 8884);
        Bootstrap bootstrap1 = new Bootstrap(config1, dispatcher());
        bootstrap1.start().block();

        MqttConfiguration config2 = config("n2", 2883, 2884, 9883, 9884);
        Bootstrap bootstrap2 = new Bootstrap(config2, dispatcher());
        bootstrap2.start().block();

        Thread.sleep(3600 * 1000);
        bootstrap1.shutdown();
        bootstrap2.shutdown();
    }

    private void setLogContext() {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        loggerContext.getLogger("root").setLevel(Level.INFO);
        loggerContext.getLogger("reactor.netty").setLevel(Level.INFO);
        loggerContext.getLogger("plus.jmqx.broker").setLevel(Level.INFO);
        loggerContext.getLogger("plus.jmqx.broker.mqtt.message.impl").setLevel(Level.DEBUG);
    }

    private MqttConfiguration config(String namespace, int mqttPort, int mqttsPort, int wsPort, int wssPort) {
        MqttConfiguration config = new MqttConfiguration();
        config.setBusinessQueueSize(Integer.MAX_VALUE);
        config.setSslEnable(true);
        config.setPort(mqttPort);
        config.setSecurePort(mqttsPort);
        config.setWebsocketPort(wsPort);
        config.setWebsocketSecurePort(wssPort);
        config.setSslCa(Objects.requireNonNull(BootstrapTest.class.getResource("/ca.crt")).getPath());
        config.setSslCrt(Objects.requireNonNull(BootstrapTest.class.getResource("/server.crt")).getPath());
        config.setSslKey(Objects.requireNonNull(BootstrapTest.class.getResource("/server.key")).getPath());
        config.getClusterConfig().setNamespace(namespace);
        return config;
    }

    private PlatformDispatcher dispatcher() {
        return new PlatformDispatcher() {
            @Override
            public Mono<Void> onConnect(ConnectMessage message) {
                return Mono.fromRunnable(() -> {
                    log.info("{}", message);
                });
            }

            @Override
            public Mono<Void> onDisconnect(DisconnectMessage message) {
                return Mono.fromRunnable(() -> {
                    log.info("{}", message);
                });
            }

            @Override
            public Mono<Void> onConnectionLost(ConnectionLostMessage message) {
                return Mono.fromRunnable(() -> {
                    log.info("{}", message);
                });
            }

            @Override
            public Mono<Void> onPublish(PublishMessage message) {
                return Mono.fromRunnable(() -> {
                    log.info("PublishMessage(clientId={}, username={}, topic={}, payload={})",
                            message.getClientId(),
                            message.getUsername(),
                            message.getTopic(),
                            new String(message.getPayload(), StandardCharsets.UTF_8));
                });
            }
        };
    }

}