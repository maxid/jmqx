package plus.jmqx.client.mqtt.stress;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import plus.jmqx.client.mqtt.v3.Mqtt3AsyncClient;
import plus.jmqx.client.mqtt.v3.Mqtt3RxClient;
import plus.jmqx.client.mqtt.v3.message.Mqtt3Subscribe;
import plus.jmqx.client.mqtt.v3.message.Mqtt3TopicFilter;
import reactor.core.Disposable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MQTT 3.1.1 jmqx-client 压力测试（三类独立场景）。
 *
 * <p>须事先启动外部 MQTT broker，默认 {@code tcp://localhost:1883}。
 * 传输层：{@code -Djmqx.client.stress.transport=tcp|mqtts|ws|wss}（端口默认 1883/8883/1884/8884）。
 * 认证：{@code -Djmqx.client.stress.broker.username=...} / {@code broker.password=...}。
 *
 * <p>按场景单独运行（推荐）：
 * <pre>
 * # 连接压测
 * mvn -pl jmqx-client test -Djmqx.stress.tests=true -Dtest=Mqtt3ClientStressTest#connectStress
 *
 * # 发布压测（尊重 qos / messages / threads / publishers）
 * mvn -pl jmqx-client test -Djmqx.stress.tests=true -Dtest=Mqtt3ClientStressTest#publishStress \
 *   -Djmqx.client.stress.messages=100000 -Djmqx.client.stress.threads=8 -Djmqx.client.stress.qos=1
 *
 * # 订阅压测（仅度量客户端接收吞吐）
 * mvn -pl jmqx-client test -Djmqx.stress.tests=true -Dtest=Mqtt3ClientStressTest#subscribeStress \
 *   -Djmqx.client.stress.messages=100000 -Djmqx.client.stress.qos=1
 * </pre>
 *
 * <p>也可用 {@code -Djmqx.client.stress.scenario=connect|publish|subscribe} 过滤整类运行时的场景。
 */
@EnabledIfSystemProperty(named = "jmqx.stress.tests", matches = "true")
class Mqtt3ClientStressTest extends ClientStressBrokerSupport {

    static {
        ClientStressLogSupport.configure();
    }

    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(30);

    private static ClientStressConfig cfg;

    @BeforeAll
    static void init() {
        cfg = ClientStressSupport.loadConfig();
    }

    /**
     * 连接压测：并发建连 → 持久保持 {@code connectionHoldSeconds} → 断开，压测同时在线连接数。
     * 参数：{@code connections}、{@code threads}、{@code connectionHoldSeconds}、{@code timeoutSeconds}
     */
    @Test
    @EnabledIf("plus.jmqx.client.mqtt.stress.StressScenarioFilter#connectEnabled")
    void connectStress() throws Exception {
        ClientStressConfig c = cfg;
        ConnectStressRunner.ConnectStats stats = ConnectStressRunner.run(stressLabel("v3"), c, clientId -> {
            Mqtt3RxClient client = newClient(clientId);
            client.connect().block(CONNECT_TIMEOUT);
            return () -> disconnectQuietly(client);
        });
        assertTrue(stats.completedInTime(), "connect stress timed out");
        assertTrue(stats.established() >= c.connections * 0.95,
                "too many connection failures: established=" + stats.established() + "/" + c.connections);
        assertEquals(stats.established(), stats.completed(),
                "not all established connections were cleanly disconnected");
    }

    /**
     * 发布压测：仅度量客户端出站发布吞吐（含 QoS ACK），无订阅者。
     * 参数：{@code messages}、{@code qos}、{@code threads}、{@code publishers}、{@code payloadBytes}
     */
    @Test
    @EnabledIf("plus.jmqx.client.mqtt.stress.StressScenarioFilter#publishEnabled")
    void publishStress() throws Exception {
        ClientStressConfig c = cfg;
        byte[] payload = ClientStressSupport.randomPayload(c.payloadBytes);
        String topic = c.topic + "/publish/" + System.nanoTime();
        int workers = Math.max(c.threads, c.publishers);
        int base = c.messages / workers;
        int remainder = c.messages % workers;
        AckAwareStressPublisher.PublishProgress progress = new AckAwareStressPublisher.PublishProgress();
        CountDownLatch latch = new CountDownLatch(workers);
        ExecutorService pool = Executors.newFixedThreadPool(workers);

        long start = System.nanoTime();
        try (StressProgressReporter progressReporter = StressProgressReporter.start(
                stressLabel("v3-publish"), c.progressIntervalSeconds, start,
                () -> ClientStressSupport.formatPublishProgress(
                        c.messages, progress.sent.get(), progress.acked.get(), progress.failed.get(), start))) {
            for (int i = 0; i < workers; i++) {
                final int idx = i;
                final int messagesPerWorker = base + (idx < remainder ? 1 : 0);
                pool.submit(() -> {
                    Mqtt3AsyncClient client = null;
                    try {
                        client = v3Async("stress-v3-pub-" + idx + "-" + System.nanoTime());
                        client.connect().get(c.timeoutSeconds, TimeUnit.SECONDS);
                        AckAwareStressPublisher.publishV3(
                                client, topic, payload, c.qos, messagesPerWorker, c.inflight, c.timeoutSeconds, progress);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        if (client != null) {
                            try {
                                client.disconnect().get(10, TimeUnit.SECONDS);
                            } catch (Exception ignored) {
                                // cleanup
                            }
                        }
                        latch.countDown();
                    }
                });
            }
            latch.await(c.timeoutSeconds, TimeUnit.SECONDS);
        }
        pool.shutdownNow();
        long end = System.nanoTime();
        long acked = progress.acked.get();
        long failed = progress.failed.get();
        ClientStressSupport.logPublishStress("v3", c, acked, failed, start, end, latch.getCount() == 0);
        assertTrue(latch.getCount() == 0, ClientStressSupport.publishStressTimeoutMessage(c, acked, failed));
        assertEquals(c.messages, acked,
                "not all messages acked by broker (acked=" + acked + ", failed=" + failed + ")");
        assertEquals(0, failed, "publish ack failures detected");
        assertTrue(ClientStressSupport.throughput(acked, start, end) >= c.minThroughputMsgPerSec,
                "publish throughput below minimum " + c.minThroughputMsgPerSec + " msg/s");
    }

    /**
     * 订阅压测：先建立订阅，再由独立发布端灌消息，仅度量客户端接收吞吐。
     * 参数：{@code messages}、{@code qos}、{@code subscribers}、{@code publishers}、{@code payloadBytes}
     */
    @Test
    @EnabledIf("plus.jmqx.client.mqtt.stress.StressScenarioFilter#subscribeEnabled")
    void subscribeStress() throws Exception {
        ClientStressConfig c = cfg;
        byte[] payload = ClientStressSupport.randomPayload(c.payloadBytes);
        String topic = c.topic + "/subscribe/" + System.nanoTime();
        long expected = (long) c.subscribers * c.messages;
        CountDownLatch latch = new CountDownLatch((int) Math.min(expected, Integer.MAX_VALUE));
        AtomicLong received = new AtomicLong();
        List<Mqtt3RxClient> subscribers = new ArrayList<>();
        List<Disposable> streams = new ArrayList<>();

        try {
            for (int i = 0; i < c.subscribers; i++) {
                Mqtt3RxClient sub = newClient("stress-v3-sub-" + i + "-" + System.nanoTime());
                subscribers.add(sub);
                sub.connect().block(CONNECT_TIMEOUT);
                Mqtt3Subscribe subscribe = Mqtt3Subscribe.builder()
                        .topicFilters(List.of(Mqtt3TopicFilter.builder().topicFilter(topic).qos(c.qos).build()))
                        .build();
                streams.add(sub.subscribePublishes(subscribe)
                        .doOnNext(p -> {
                            if (p.getQoS().value() > 0) {
                                p.ack();
                            }
                            received.incrementAndGet();
                            latch.countDown();
                        })
                        .subscribe());
                sub.subscribe(subscribe).block(CONNECT_TIMEOUT);
            }

            long start = System.nanoTime();
            boolean ok;
            try (StressProgressReporter progressReporter = StressProgressReporter.start(
                    stressLabel("v3-subscribe"), c.progressIntervalSeconds, start,
                    () -> ClientStressSupport.formatSubscribeProgress(expected, received.get(), start))) {
                publishAll(topic, payload, c);
                ok = latch.await(c.timeoutSeconds, TimeUnit.SECONDS);
            }
            long end = System.nanoTime();
            ClientStressSupport.logSubscribeStress("v3", c, received.get(), start, end, ok);
            assertTrue(ok, ClientStressSupport.subscribeStressTimeoutMessage(c, received.get(), expected));
            assertEquals(expected, received.get(), "not all messages received by subscribers");
            assertTrue(ClientStressSupport.throughput(received.get(), start, end) >= c.minThroughputMsgPerSec,
                    "subscribe throughput below minimum " + c.minThroughputMsgPerSec + " msg/s");
        } finally {
            streams.forEach(Disposable::dispose);
            subscribers.forEach(ClientStressBrokerSupport::disconnectQuietly);
        }
    }

    private static void publishAll(String topic, byte[] payload, ClientStressConfig c) throws Exception {
        List<Mqtt3AsyncClient> publishers = new ArrayList<>();
        try {
            for (int i = 0; i < c.publishers; i++) {
                Mqtt3AsyncClient pub = v3Async("stress-v3-feed-" + i + "-" + System.nanoTime());
                pub.connect().get(c.timeoutSeconds, TimeUnit.SECONDS);
                publishers.add(pub);
            }
            int base = c.messages / publishers.size();
            int remainder = c.messages % publishers.size();
            long acked = 0;
            long failed = 0;
            for (int i = 0; i < publishers.size(); i++) {
                int count = base + (i < remainder ? 1 : 0);
                AckAwareStressPublisher.PublishStats stats = AckAwareStressPublisher.publishV3(
                        publishers.get(i), topic, payload, c.qos, count, c.inflight, c.timeoutSeconds);
                acked += stats.acked();
                failed += stats.failed();
            }
            assertEquals(c.messages, acked, "feed publisher did not ack all messages");
            assertEquals(0, failed, "feed publisher ack failures");
        } finally {
            for (Mqtt3AsyncClient pub : publishers) {
                try {
                    pub.disconnect().get(10, TimeUnit.SECONDS);
                } catch (Exception ignored) {
                    // cleanup
                }
            }
        }
    }

    private static String stressLabel(String suffix) {
        return cfg.transport.label() + "-" + suffix;
    }

    private static Mqtt3RxClient newClient(String clientId) {
        return v3Rx(clientId);
    }

}
