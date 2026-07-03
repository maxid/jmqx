package plus.jmqx.client.mqtt.stress;

import lombok.extern.slf4j.Slf4j;
import plus.jmqx.client.mqtt.message.QoS;

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

    public static String brokerHost() {
        String stress = System.getProperty("jmqx.client.stress.broker.host");
        if (stress != null && !stress.isEmpty()) {
            return stress;
        }
        return System.getProperty("jmqx.it.broker.host", "localhost");
    }

    public static int brokerPort() {
        String stress = System.getProperty("jmqx.client.stress.broker.port");
        if (stress != null && !stress.isEmpty()) {
            return Integer.parseInt(stress);
        }
        String it = System.getProperty("jmqx.it.broker.port");
        if (it != null && !it.isEmpty()) {
            return Integer.parseInt(it);
        }
        return 1883;
    }

    public static ClientStressConfig loadConfig() {
        ClientStressConfig c = new ClientStressConfig();
        c.brokerHost = brokerHost();
        c.brokerPort = brokerPort();
        c.messages = intProp("jmqx.client.stress.messages", 2_000);
        c.threads = intProp("jmqx.client.stress.threads", 4);
        c.subscribers = intProp("jmqx.client.stress.subscribers", 1);
        c.publishers = intProp("jmqx.client.stress.publishers", 1);
        c.connections = intProp("jmqx.client.stress.connections", 50);
        c.payloadBytes = intProp("jmqx.client.stress.payloadBytes", 64);
        c.qos = QoS.fromValue(intProp("jmqx.client.stress.qos", 0));
        c.minThroughputMsgPerSec = intProp("jmqx.client.stress.minThroughput", 100);
        c.inflight = intProp("jmqx.client.stress.inflight", 256);
        c.timeoutSeconds = intProp("jmqx.client.stress.timeoutSeconds", 120);
        c.progressIntervalSeconds = intProp("jmqx.client.stress.progressIntervalSeconds", 5);
        c.topic = System.getProperty("jmqx.client.stress.topic", "stress/client/topic");
        return c;
    }

    public static byte[] randomPayload(int bytes) {
        byte[] payload = new byte[bytes];
        ThreadLocalRandom.current().nextBytes(payload);
        return payload;
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
        log.info("{} subscribe stress: subscribers={}, publishers={}, messages={}, qos={}, received={}, time={}s, throughput={} msg/s, completed={}",
                label,
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
        log.info("{} publish stress: target={}, acked={}, failed={}, inflight={}, threads={}, publishers={}, qos={}, payload={}B, time={}s, throughput={} msg/s, completed={}",
                label,
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

    public static String formatConnectProgress(long target, long success, long startNanos) {
        double sec = Math.max((System.nanoTime() - startNanos) / 1_000_000_000.0, 0.001);
        double pct = target == 0 ? 100.0 : success * 100.0 / target;
        return String.format("target=%d, success=%d, throughput=%.0f conn/s, progress=%.1f%%",
                target, success, success / sec, pct);
    }

}
