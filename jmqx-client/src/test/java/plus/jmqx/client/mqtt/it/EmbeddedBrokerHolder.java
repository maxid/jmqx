package plus.jmqx.client.mqtt.it;

import plus.jmqx.broker.Bootstrap;
import plus.jmqx.broker.mqtt.MqttConfiguration;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;

/**
 * 集成测试用内嵌 jmqx-broker 生命周期管理。
 *
 * <p>默认在随机端口启动 broker；若设置了 {@code -Djmqx.it.broker.port=1883} 则使用外部 broker。
 */
final class EmbeddedBrokerHolder {

    private static final Object   LOCK      = new Object();
    private static volatile Bootstrap bootstrap;
    private static volatile int     port;

    private EmbeddedBrokerHolder() {
    }

    static String host() {
        return System.getProperty("jmqx.it.broker.host", "localhost");
    }

    static int port() {
        String external = System.getProperty("jmqx.it.broker.port");
        if (external != null && !external.isEmpty()) {
            return Integer.parseInt(external);
        }
        ensureStarted();
        return port;
    }

    static void ensureStarted() {
        if (bootstrap != null) {
            return;
        }
        synchronized (LOCK) {
            if (bootstrap != null) {
                return;
            }
            if (System.getProperty("jmqx.it.broker.port") != null) {
                return;
            }
            port = randomPort();
            MqttConfiguration config = new MqttConfiguration();
            config.setBusinessQueueSize(Integer.MAX_VALUE);
            config.setSslEnable(false);
            config.setPort(port);
            config.setSecurePort(-1);
            config.setWebsocketPort(-1);
            config.setWebsocketSecurePort(-1);
            config.setWiretap(false);
            config.getClusterConfig().setNamespace("jmqx-client-it-" + port);
            config.getClusterConfig().setNode("");
            bootstrap = new Bootstrap(config);
            try {
                bootstrap.start().block(Duration.ofSeconds(30));
            } catch (Exception e) {
                throw new IllegalStateException("failed to start embedded broker on port " + port, e);
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
            }
        }
    }

    private static int randomPort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new IllegalStateException("cannot allocate port for embedded broker", e);
        }
    }

}
