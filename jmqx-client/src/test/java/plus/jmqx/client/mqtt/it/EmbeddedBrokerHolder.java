package plus.jmqx.client.mqtt.it;

import plus.jmqx.broker.Bootstrap;
import plus.jmqx.broker.mqtt.MqttConfiguration;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.Objects;

/**
 * 集成测试用内嵌 jmqx-broker 生命周期管理。
 *
 * <p>默认在随机端口启动 broker（TCP + MQTTS + WS + WSS）；若设置了任一 {@code jmqx.it.broker.*Port}
 * 则使用外部 broker，各端口默认与 jmqx-broker 一致（1883 / 8883 / 1884 / 8884）。
 */
final class EmbeddedBrokerHolder {

    /** jmqx-broker 默认 MQTT TCP 端口 */
    static final int DEFAULT_PORT                  = 1883;
    /** jmqx-broker 默认 MQTTS 端口 */
    static final int DEFAULT_SECURE_PORT           = 8883;
    /** jmqx-broker 默认 WS 端口 */
    static final int DEFAULT_WEBSOCKET_PORT        = 1884;
    /** jmqx-broker 默认 WSS 端口 */
    static final int DEFAULT_WEBSOCKET_SECURE_PORT = 8884;

    private static final    Object             LOCK = new Object();
    private static volatile Bootstrap          bootstrap;
    private static volatile MqttConfiguration  mqttConfig;

    private EmbeddedBrokerHolder() {
    }

    static String host() {
        return System.getProperty("jmqx.it.broker.host", "localhost");
    }

    static int port() {
        return resolvePort("jmqx.it.broker.port", DEFAULT_PORT, false,
                () -> mqttConfig.getPort());
    }

    static int securePort() {
        return resolvePort("jmqx.it.broker.securePort", DEFAULT_SECURE_PORT, true,
                () -> mqttConfig.getSecurePort());
    }

    static int websocketPort() {
        return resolvePort("jmqx.it.broker.websocketPort", DEFAULT_WEBSOCKET_PORT, false,
                () -> mqttConfig.getWebsocketPort());
    }

    static int websocketSecurePort() {
        return resolvePort("jmqx.it.broker.websocketSecurePort", DEFAULT_WEBSOCKET_SECURE_PORT, true,
                () -> mqttConfig.getWebsocketSecurePort());
    }

    static void ensureStarted() {
        if (bootstrap != null) {
            return;
        }
        synchronized (LOCK) {
            if (bootstrap != null) {
                return;
            }
            if (usesExternalBroker()) {
                return;
            }
            mqttConfig = createEmbeddedConfig();
            bootstrap = new Bootstrap(mqttConfig);
            try {
                bootstrap.start().block(Duration.ofSeconds(30));
            } catch (Exception e) {
                throw new IllegalStateException("failed to start embedded broker", e);
            }
            Runtime.getRuntime().addShutdownHook(new Thread(EmbeddedBrokerHolder::shutdownQuietly));
        }
    }

    static void shutdownQuietly() {
        synchronized (LOCK) {
            if (bootstrap != null) {
                try {
                    bootstrap.shutdown();
                } catch (Exception ignored) {
                    // test JVM shutdown
                }
                bootstrap = null;
                mqttConfig = null;
            }
        }
    }

    private static MqttConfiguration createEmbeddedConfig() {
        MqttConfiguration config = new MqttConfiguration();
        config.setBusinessQueueSize(Integer.MAX_VALUE);
        config.setSslEnable(true);
        config.setPort(randomPort());
        config.setSecurePort(randomPort());
        config.setWebsocketPort(randomPort());
        config.setWebsocketSecurePort(randomPort());
        config.setWiretap(false);
        config.setSslCa(resourcePath("/ssl/ca.crt"));
        config.setSslCrt(resourcePath("/ssl/server.crt"));
        config.setSslKey(resourcePath("/ssl/server.key"));
        config.getClusterConfig().setNamespace("jmqx-client-it-" + config.getPort());
        config.getClusterConfig().setNode("");
        return config;
    }

    private static String resourcePath(String resource) {
        return Objects.requireNonNull(EmbeddedBrokerHolder.class.getResource(resource),
                "missing test resource: " + resource).getPath();
    }

    private static int resolvePort(String property, int defaultPort, boolean requiresSsl,
                                   IntSupplier embeddedPort) {
        String external = System.getProperty(property);
        if (external != null && !external.isEmpty()) {
            return Integer.parseInt(external);
        }
        if (usesExternalBroker()) {
            return defaultPort;
        }
        ensureStarted();
        if (requiresSsl && !mqttConfig.getSslEnable()) {
            throw new IllegalStateException("embedded broker SSL is disabled, cannot resolve " + property);
        }
        return embeddedPort.getAsInt();
    }

    private static boolean usesExternalBroker() {
        return System.getProperty("jmqx.it.broker.port") != null
                || System.getProperty("jmqx.it.broker.securePort") != null
                || System.getProperty("jmqx.it.broker.websocketPort") != null
                || System.getProperty("jmqx.it.broker.websocketSecurePort") != null;
    }

    private static int randomPort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new IllegalStateException("cannot allocate port for embedded broker", e);
        }
    }

    @FunctionalInterface
    private interface IntSupplier {
        int getAsInt();
    }

}
