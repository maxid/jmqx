package plus.jmqx.client.mqtt.stress;

import lombok.extern.slf4j.Slf4j;
import plus.jmqx.client.mqtt.MqttClientConfig;
import plus.jmqx.client.mqtt.message.QoS;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadLocalRandom;

/**
 * jmqx-client 压测工具。
 */
@Slf4j
public final class ClientStressSupport {

    private ClientStressSupport() {
    }

    public static int intProp(String key, int def) {
        String value = System.getProperty(key);
        if (value == null || value.isEmpty()) {
            return def;
        }
        return Integer.parseInt(value);
    }

    public static String stringProp(String key) {
        String value = System.getProperty(key);
        if (value == null || value.isEmpty()) {
            return null;
        }
        return value;
    }

    /**
     * MQTT 认证用户名（未设置则匿名连接）。
     *
     * <p>{@code jmqx.client.stress.broker.username}，兼容 {@code jmqx.client.stress.username}。
     */
    public static String username() {
        String stress = stringProp("jmqx.client.stress.broker.username");
        if (stress != null) {
            return stress;
        }
        return stringProp("jmqx.client.stress.username");
    }

    /**
     * MQTT 认证密码（未设置则为 null）。
     *
     * <p>{@code jmqx.client.stress.broker.password}，兼容 {@code jmqx.client.stress.password}。
     */
    public static byte[] password() {
        String stress = stringProp("jmqx.client.stress.broker.password");
        if (stress == null) {
            stress = stringProp("jmqx.client.stress.password");
        }
        return stress != null ? stress.getBytes(StandardCharsets.UTF_8) : null;
    }

    public static String brokerHost() {
        String stress = System.getProperty("jmqx.client.stress.broker.host");
        if (stress != null && !stress.isEmpty()) {
            return stress;
        }
        return System.getProperty("jmqx.it.broker.host", "localhost");
    }

    public static int brokerPort() {
        return brokerPort(transport());
    }

    public static ClientStressTransport transport() {
        return ClientStressTransport.parse(System.getProperty("jmqx.client.stress.transport", "tcp"));
    }

    public static int brokerPort(ClientStressTransport transport) {
        String stress = System.getProperty(transport.stressPortProperty());
        if (stress != null && !stress.isEmpty()) {
            return Integer.parseInt(stress);
        }
        String it = System.getProperty(transport.itPortProperty());
        if (it != null && !it.isEmpty()) {
            return Integer.parseInt(it);
        }
        if (transport == ClientStressTransport.TCP) {
            return 1883;
        }
        return transport.defaultPort();
    }

    /**
     * 是否显式设置了 {@code jmqx.client.stress.timeoutSeconds}。
     */
    public static boolean hasExplicitTimeoutSeconds() {
        String value = System.getProperty("jmqx.client.stress.timeoutSeconds");
        return value != null && !value.isEmpty();
    }

    /**
     * 解析压测超时：显式配置优先；否则按消息量自动估算（保守按 5 万 msg/s，1.3 倍余量）。
     */
    public static int resolveTimeoutSeconds(int messages) {
        if (hasExplicitTimeoutSeconds()) {
            return intProp("jmqx.client.stress.timeoutSeconds", 120);
        }
        return autoTimeoutSeconds(messages);
    }

    static int autoTimeoutSeconds(int messages) {
        if (messages <= 50_000) {
            return 120;
        }
        long seconds = (long) Math.ceil(messages / 50_000.0 * 1.3) + 30;
        return (int) Math.min(Math.max(120, seconds), 86_400);
    }

    public static ClientStressConfig loadConfig() {
        ClientStressConfig c = new ClientStressConfig();
        c.transport = transport();
        c.brokerHost = brokerHost();
        c.brokerPort = brokerPort(c.transport);
        c.username = username();
        c.password = password();
        c.messages = intProp("jmqx.client.stress.messages", 2_000);
        c.threads = intProp("jmqx.client.stress.threads", 4);
        c.subscribers = intProp("jmqx.client.stress.subscribers", 1);
        c.publishers = intProp("jmqx.client.stress.publishers", 1);
        c.connections = intProp("jmqx.client.stress.connections", 50);
        c.connectionHoldSeconds = intProp("jmqx.client.stress.connectionHoldSeconds", 30);
        c.payloadBytes = intProp("jmqx.client.stress.payloadBytes", 64);
        c.qos = QoS.fromValue(intProp("jmqx.client.stress.qos", 0));
        c.minThroughputMsgPerSec = intProp("jmqx.client.stress.minThroughput", 100);
        c.inflight = intProp("jmqx.client.stress.inflight", 256);
        c.timeoutSeconds = resolveTimeoutSeconds(c.messages);
        if (!hasExplicitTimeoutSeconds() && c.messages > 50_000) {
            log.info("stress timeout auto: {}s (messages={}; override with -Djmqx.client.stress.timeoutSeconds)",
                    c.timeoutSeconds, c.messages);
        }
        c.progressIntervalSeconds = intProp("jmqx.client.stress.progressIntervalSeconds", 5);
        c.topic = System.getProperty("jmqx.client.stress.topic", "stress/client/topic");
        return c;
    }

    public static byte[] randomPayload(int bytes) {
        byte[] payload = new byte[bytes];
        ThreadLocalRandom.current().nextBytes(payload);
        return payload;
    }

    public static void logConnectStress(String label, ClientStressConfig config, ConnectStressRunner.ConnectStats stats,
                                        long startNanos, long endNanos, boolean completed) {
        double seconds = Math.max((endNanos - startNanos) / 1_000_000_000.0, 0.001);
        log.info("{} connect stress [{}://{}:{}]: connections={}, hold={}s, threads={}, established={}, peakActive={}, completed={}, failed={}, time={}s, completed={}",
                label,
                config.transport.label(),
                config.brokerHost,
                config.brokerPort,
                config.connections,
                config.connectionHoldSeconds,
                config.threads,
                stats.established(),
                stats.peakActive(),
                stats.completed(),
                stats.failed(),
                String.format("%.3f", seconds),
                completed);
    }

    public static void logConnectStress(String label, ClientStressConfig config, long success,
                                        long startNanos, long endNanos, boolean completed) {
        double seconds = Math.max((endNanos - startNanos) / 1_000_000_000.0, 0.001);
        double throughput = success / seconds;
        log.info("{} connect stress: connections={}, threads={}, success={}, time={}s, throughput={} conn/s, completed={}",
                label,
                config.connections,
                config.threads,
                success,
                String.format("%.3f", seconds),
                String.format("%.0f", throughput),
                completed);
    }

    public static void logSubscribeStress(String label, ClientStressConfig config, long received,
                                          long startNanos, long endNanos, boolean completed) {
        double seconds = Math.max((endNanos - startNanos) / 1_000_000_000.0, 0.001);
        double throughput = received / seconds;
        log.info("{} subscribe stress [{}://{}:{}]: subscribers={}, publishers={}, messages={}, qos={}, received={}, time={}s, throughput={} msg/s, completed={}",
                label,
                config.transport.label(),
                config.brokerHost,
                config.brokerPort,
                config.subscribers,
                config.publishers,
                config.messages,
                config.qos,
                received,
                String.format("%.3f", seconds),
                String.format("%.0f", throughput),
                completed);
    }

    public static void logPublishStress(String label, ClientStressConfig config, long acked, long failed,
                                        long startNanos, long endNanos, boolean completed) {
        double seconds = Math.max((endNanos - startNanos) / 1_000_000_000.0, 0.001);
        double throughput = acked / seconds;
        log.info("{} publish stress [{}://{}:{}]: target={}, acked={}, failed={}, inflight={}, threads={}, publishers={}, qos={}, payload={}B, time={}s, throughput={} msg/s, completed={}",
                label,
                config.transport.label(),
                config.brokerHost,
                config.brokerPort,
                config.messages,
                acked,
                failed,
                config.inflight,
                config.threads,
                config.publishers,
                config.qos,
                config.payloadBytes,
                String.format("%.3f", seconds),
                String.format("%.0f", throughput),
                completed);
    }

    public static void logThroughput(String label, ClientStressConfig config, long count,
                                     long startNanos, long endNanos, boolean completed) {
        logPublishStress(label, config, count, 0, startNanos, endNanos, completed);
    }

    public static void logPubSub(String label, ClientStressConfig config, long published, long received,
                                 long startNanos, long endNanos, boolean completed) {
        double seconds = Math.max((endNanos - startNanos) / 1_000_000_000.0, 0.001);
        log.info("{} pub/sub stress: publishers={}, subscribers={}, messages={}, qos={}, published={}, received={}, time={}s, delivery={}%, completed={}",
                label,
                config.publishers,
                config.subscribers,
                config.messages,
                config.qos,
                published,
                received,
                String.format("%.3f", seconds),
                published == 0 ? "0" : String.format("%.1f", received * 100.0 / published),
                completed);
    }

    public static double throughput(long count, long startNanos, long endNanos) {
        double seconds = Math.max((endNanos - startNanos) / 1_000_000_000.0, 0.001);
        return count / seconds;
    }

    public static double throughput(long count, long startNanos) {
        return throughput(count, startNanos, System.nanoTime());
    }

    public static String formatPublishProgress(long target, long sent, long acked, long failed, long startNanos) {
        double sec = Math.max((System.nanoTime() - startNanos) / 1_000_000_000.0, 0.001);
        double pct = target == 0 ? 100.0 : acked * 100.0 / target;
        return String.format("target=%d, sent=%d, acked=%d, failed=%d, throughput=%.0f msg/s, progress=%.1f%%",
                target, sent, acked, failed, acked / sec, pct);
    }

    public static String formatSubscribeProgress(long target, long received, long startNanos) {
        double sec = Math.max((System.nanoTime() - startNanos) / 1_000_000_000.0, 0.001);
        double pct = target == 0 ? 100.0 : received * 100.0 / target;
        return String.format("target=%d, received=%d, throughput=%.0f msg/s, progress=%.1f%%",
                target, received, received / sec, pct);
    }

    public static String formatConnectProgress(long target, long active, long established, long completed,
                                               long startNanos) {
        double sec = Math.max((System.nanoTime() - startNanos) / 1_000_000_000.0, 0.001);
        return String.format("target=%d, active=%d, established=%d, completed=%d, elapsed=%.1fs",
                target, active, established, completed, sec);
    }

    public static String formatConnectProgress(long target, long success, long startNanos) {
        double sec = Math.max((System.nanoTime() - startNanos) / 1_000_000_000.0, 0.001);
        double pct = target == 0 ? 100.0 : success * 100.0 / target;
        return String.format("target=%d, success=%d, throughput=%.0f conn/s, progress=%.1f%%",
                target, success, success / sec, pct);
    }

    public static String publishStressTimeoutMessage(ClientStressConfig c, long acked, long failed) {
        return "publish stress did not finish within " + c.timeoutSeconds + "s (acked=" + acked + "/"
                + c.messages + ", failed=" + failed + "); set -Djmqx.client.stress.timeoutSeconds higher";
    }

    public static String subscribeStressTimeoutMessage(ClientStressConfig c, long received, long expected) {
        return "subscribe stress did not finish within " + c.timeoutSeconds + "s (received=" + received + "/"
                + expected + "); set -Djmqx.client.stress.timeoutSeconds higher";
    }

}
