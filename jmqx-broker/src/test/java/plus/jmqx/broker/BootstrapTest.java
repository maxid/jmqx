package plus.jmqx.broker;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.resolver.NoopAddressResolverGroup;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.slf4j.LoggerFactory;
import plus.jmqx.broker.mqtt.MqttConfiguration;
import plus.jmqx.broker.mqtt.message.MqttMessageBuilder;
import plus.jmqx.broker.mqtt.message.dispatch.*;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.tcp.TcpClient;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * MQTT Broker 测试用例
 *
 * @author maxid
 * @since 2025/4/22 10:46
 */
@Slf4j
@EnabledIfSystemProperty(named = "jmqx.integration.tests", matches = "true")
class BootstrapTest {

    /**
     * 验证多个 Broker 实例启动与关闭。
     *
     * @throws Exception 测试异常
     */
    @Test
    void testBroker() throws Exception {
        setLogContext();

        MqttConfiguration config1 = config("n1", 1883, 1884, 8883, 8884);
        Bootstrap bootstrap1 = new Bootstrap(config1, dispatcher());
        bootstrap1.start().block();

        MqttConfiguration config2 = config("n2", 2883, 2884, 9883, 9884);
        Bootstrap bootstrap2 = new Bootstrap(config2, dispatcher());
        bootstrap2.start().block();

        Thread.sleep(intProp("jmqx.test.await.seconds", 5) * TimeUnit.SECONDS.toMillis(1));

        bootstrap1.shutdown();
        bootstrap2.shutdown();
    }

    /**
     * 运行 Broker 压测并输出结果。
     *
     * @throws Exception 测试异常
     */
    @Test
    void testBrokerStress() throws Exception {
        setLogContext();

        StressConfig stress = loadStressConfig();
        AtomicLong dispatchReceived = new AtomicLong();
        MqttConfiguration config1 = stressConfig(stress.port);
        Bootstrap bootstrap1 = new Bootstrap(config1, stressDispatcher(dispatchReceived));
        bootstrap1.start().block();

        StressResult result = runStress(stress, dispatchReceived);
        logStressResult(stress, result, dispatchReceived.get());

        bootstrap1.shutdown();
    }

    /**
     * 设置测试日志级别。
     */
    private void setLogContext() {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        loggerContext.getLogger("root").setLevel(Level.INFO);
        loggerContext.getLogger("reactor.netty").setLevel(Level.INFO);
        loggerContext.getLogger("plus.jmqx.broker").setLevel(Level.INFO);
        loggerContext.getLogger("plus.jmqx.broker.mqtt.message.impl").setLevel(Level.DEBUG);
    }

    /**
     * 创建基础配置。
     *
     * @param namespace 命名空间
     * @param mqttPort  MQTT 端口
     * @param mqttsPort MQTTS 端口
     * @param wsPort    WS 端口
     * @param wssPort   WSS 端口
     * @return MQTT 配置
     */
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

    /**
     * 创建压测配置。
     *
     * @param mqttPort MQTT 端口
     * @return MQTT 配置
     */
    private MqttConfiguration stressConfig(int mqttPort) {
        MqttConfiguration config = new MqttConfiguration();
        config.setBusinessQueueSize(Integer.MAX_VALUE);
        config.setSslEnable(false);
        config.setPort(mqttPort);
        config.setSecurePort(-1);
        config.setWebsocketPort(-1);
        config.setWebsocketSecurePort(-1);
        config.getClusterConfig().setNamespace("jmqx-stress-" + UUID.randomUUID());
        return config;
    }

    /**
     * 读取整型系统属性。
     *
     * @param key 属性名
     * @param def 默认值
     * @return 属性值
     */
    private static int intProp(String key, int def) {
        String value = System.getProperty(key);
        if (value == null || value.isEmpty()) {
            return def;
        }
        return Integer.parseInt(value);
    }

    /**
     * 加载压测参数。
     *
     * @return 压测配置
     */
    private StressConfig loadStressConfig() {
        StressConfig config = new StressConfig();
        config.port = intProp("jmqx.stress.port", 1883);
        config.threads = intProp("jmqx.stress.threads", 4);
        config.durationSeconds = intProp("jmqx.stress.durationSeconds", 600);
        config.payloadBytes = intProp("jmqx.stress.payloadBytes", 64);
        config.flushEvery = intProp("jmqx.stress.flushEvery", 256);
        config.inFlightLimit = intProp("jmqx.stress.inFlightLimit", 20000);
        config.timeoutSeconds = intProp("jmqx.stress.timeoutSeconds", 720);
        config.reportIntervalSeconds = intProp("jmqx.stress.reportIntervalSeconds", 10);
        config.topic = "stress/topic";
        return config;
    }

    /**
     * 执行压测任务。
     *
     * @param config           压测配置
     * @param dispatchReceived 分发计数
     * @return 压测结果
     * @throws InterruptedException 中断异常
     */
    private StressResult runStress(StressConfig config, AtomicLong dispatchReceived) throws InterruptedException {
        AtomicLong published = new AtomicLong();
        AtomicLong acked = new AtomicLong();
        CountDownLatch latch = new CountDownLatch(config.threads);
        ExecutorService executor = Executors.newFixedThreadPool(config.threads);
        ScheduledExecutorService reporter = Executors.newSingleThreadScheduledExecutor();
        long start = System.nanoTime();
        AtomicLong lastAcked = new AtomicLong();
        AtomicLong lastReportAt = new AtomicLong(start);
        reporter.scheduleAtFixedRate(() -> logProgress(config, published, acked, dispatchReceived, lastAcked, lastReportAt, start),
                config.reportIntervalSeconds,
                config.reportIntervalSeconds,
                TimeUnit.SECONDS);
        if (!preflightPublish(config, acked, dispatchReceived)) {
            reporter.shutdownNow();
            executor.shutdownNow();
            return new StressResult(acked.get(), start, System.nanoTime(), false);
        }

        for (int i = 0; i < config.threads; i++) {
            final String clientId = "stress-" + i;
            executor.submit(() -> {
                try {
                    MqttStressClient client = new MqttStressClient(clientId, config.port, acked);
                    client.connect();
                    long sentCount = client.publishLoop(config.topic, config.payloadBytes, config.durationSeconds, config.flushEvery, config.inFlightLimit, published);
                    client.awaitAcks(acked, sentCount, config.timeoutSeconds);
                    client.close();
                } catch (Exception e) {
                    log.error("stress client error", e);
                } finally {
                    latch.countDown();
                }
            });
        }

        boolean ok = latch.await(config.timeoutSeconds, TimeUnit.SECONDS);
        long end = System.nanoTime();
        executor.shutdownNow();
        reporter.shutdownNow();
        return new StressResult(acked.get(), start, end, ok);
    }

    /**
     * 输出压测汇总结果。
     *
     * @param config           压测配置
     * @param result           压测结果
     * @param dispatchReceived 分发计数
     */
    private void logStressResult(StressConfig config, StressResult result, long dispatchReceived) {
        double seconds = (result.endNanos - result.startNanos) / 1_000_000_000.0;
        double throughput = result.sent / Math.max(seconds, 0.001);
        log.info("broker stress: threads={}, durationSeconds={}, payloadBytes={}, acked={}, dispatchReceived={}, time={}s, throughput={} msg/s, completed={}",
                config.threads,
                config.durationSeconds,
                config.payloadBytes,
                result.sent,
                dispatchReceived,
                String.format("%.3f", seconds),
                String.format("%.0f", throughput),
                result.completed);
    }

    /**
     * 执行压测预检发布。
     *
     * @param config           压测配置
     * @param acked            Ack 计数
     * @param dispatchReceived 分发计数
     * @return 是否预检成功
     */
    private boolean preflightPublish(StressConfig config, AtomicLong acked, AtomicLong dispatchReceived) {
        MqttStressClient client = new MqttStressClient("stress-preflight", config.port, acked);
        try {
            client.connect();
            long beforeDispatch = dispatchReceived.get();
            boolean ok = client.publishOnceWaitAck(config.topic, config.payloadBytes, 5);
            if (!ok) {
                log.error("preflight publish did not receive PUBACK");
                return false;
            }
            if (!waitForDispatch(dispatchReceived, beforeDispatch, 5)) {
                log.error("preflight publish did not reach dispatcher");
                return false;
            }
            return ok;
        } catch (Exception e) {
            log.error("preflight publish failed", e);
            return false;
        } finally {
            client.close();
        }
    }

    /**
     * 等待分发计数增长。
     *
     * @param dispatchReceived 分发计数
     * @param beforeDispatch   初始计数
     * @param timeoutSeconds   超时秒数
     * @return 是否成功
     */
    private boolean waitForDispatch(AtomicLong dispatchReceived, long beforeDispatch, int timeoutSeconds) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
        while (System.nanoTime() < deadline) {
            if (dispatchReceived.get() > beforeDispatch) {
                return true;
            }
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return false;
    }

    /**
     * 输出压测实时进度。
     *
     * @param config           压测配置
     * @param published        发布计数
     * @param acked            Ack 计数
     * @param dispatchReceived 分发计数
     * @param lastAcked        上次 Ack 计数
     * @param lastReportAt     上次报告时间
     * @param startNanos       起始时间
     */
    private void logProgress(StressConfig config,
                             AtomicLong published,
                             AtomicLong acked,
                             AtomicLong dispatchReceived,
                             AtomicLong lastAcked,
                             AtomicLong lastReportAt,
                             long startNanos) {
        long now = System.nanoTime();
        long totalAcked = acked.get();
        long totalPublished = published.get();
        long totalDispatch = dispatchReceived.get();
        long lastTotal = lastAcked.getAndSet(totalAcked);
        long lastAt = lastReportAt.getAndSet(now);
        double intervalSeconds = Math.max((now - lastAt) / 1_000_000_000.0, 0.001);
        double intervalThroughput = (totalAcked - lastTotal) / intervalSeconds;
        double elapsedSeconds = Math.max((now - startNanos) / 1_000_000_000.0, 0.001);
        log.info("broker stress progress: threads={}, published={}, acked={}, dispatchReceived={}, intervalThroughput={} msg/s, elapsed={}s",
                config.threads,
                totalPublished,
                totalAcked,
                totalDispatch,
                String.format("%.0f", intervalThroughput),
                String.format("%.1f", elapsedSeconds));
    }

    /**
     * 构造测试用消息分发器。
     *
     * @return 分发器
     */
    private PlatformDispatcher dispatcher() {
        return new PlatformDispatcher() {
            /**
             * 处理连接消息。
             *
             * @param message 连接消息
             * @return 处理结果
             */
            @Override
            public Mono<Void> onConnect(ConnectMessage message) {
                return Mono.fromRunnable(() -> {
                    log.info("{}", message);
                });
            }

            /**
             * 处理断开连接消息。
             *
             * @param message 断开消息
             * @return 处理结果
             */
            @Override
            public Mono<Void> onDisconnect(DisconnectMessage message) {
                return Mono.fromRunnable(() -> {
                    log.info("{}", message);
                });
            }

            /**
             * 处理连接丢失消息。
             *
             * @param message 连接丢失消息
             * @return 处理结果
             */
            @Override
            public Mono<Void> onConnectionLost(ConnectionLostMessage message) {
                return Mono.fromRunnable(() -> {
                    log.info("{}", message);
                });
            }

            /**
             * 处理发布消息。
             *
             * @param message 发布消息
             * @return 处理结果
             */
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

    /**
     * 构造压测用分发器。
     *
     * @param dispatchReceived 分发计数
     * @return 分发器
     */
    private PlatformDispatcher stressDispatcher(AtomicLong dispatchReceived) {
        return new PlatformDispatcher() {
            /**
             * 处理连接消息。
             *
             * @param message 连接消息
             * @return 处理结果
             */
            @Override
            public Mono<Void> onConnect(ConnectMessage message) {
                return Mono.fromRunnable(() -> {});
            }

            /**
             * 处理断开连接消息。
             *
             * @param message 断开消息
             * @return 处理结果
             */
            @Override
            public Mono<Void> onDisconnect(DisconnectMessage message) {
                return Mono.fromRunnable(() -> {});
            }

            /**
             * 处理连接丢失消息。
             *
             * @param message 连接丢失消息
             * @return 处理结果
             */
            @Override
            public Mono<Void> onConnectionLost(ConnectionLostMessage message) {
                return Mono.fromRunnable(() -> {});
            }

            /**
             * 处理发布消息并计数。
             *
             * @param message 发布消息
             * @return 处理结果
             */
            @Override
            public Mono<Void> onPublish(PublishMessage message) {
                return Mono.fromRunnable(dispatchReceived::incrementAndGet);
            }
        };
    }

    private static final class MqttStressClient {
        private final String clientId;
        private final int port;
        private final AtomicLong ackedCounter;
        private Connection connection;
        private final ConcurrentLinkedQueue<MqttMessage> inbox = new ConcurrentLinkedQueue<>();
        private int packetId = 1;
        private final AtomicLong localAcked = new AtomicLong();

        /**
         * 构造压测客户端。
         *
         * @param clientId      客户端 ID
         * @param port          端口
         * @param ackedCounter  Ack 计数
         */
        private MqttStressClient(String clientId, int port, AtomicLong ackedCounter) {
            this.clientId = clientId;
            this.port = port;
            this.ackedCounter = ackedCounter;
        }

        /**
         * 建立连接并完成握手。
         */
        void connect() {
            this.connection = TcpClient.create()
                    .resolver(NoopAddressResolverGroup.INSTANCE)
                    .remoteAddress(() -> new InetSocketAddress("127.0.0.1", port))
                    .connectNow(Duration.ofSeconds(5));
            ensureMqttPipeline();

            MqttMessage connect = MqttMessageBuilder.connectMessage(
                    clientId,
                    "",
                    "",
                    "",
                    "",
                    false,
                    false,
                    false,
                    0,
                    60
            );
            writeAndFlush(connect);
            connection.inbound()
                    .receiveObject()
                    .ofType(MqttMessage.class)
                    .subscribe(this::onInbound);
            MqttConnAckMessage ack = (MqttConnAckMessage) awaitMessage(
                    msg -> msg instanceof MqttConnAckMessage,
                    Duration.ofSeconds(5)
            );
            if (ack == null) {
                throw new IllegalStateException("connect ack timeout");
            }
        }

        /**
         * 循环发布消息直到超时。
         *
         * @param topic          主题
         * @param payloadBytes   负载大小
         * @param durationSeconds 运行时长
         * @param flushEvery     刷新间隔
         * @param inFlightLimit  飞行窗口
         * @param published      发布计数
         * @return 本地发送数量
         */
        long publishLoop(String topic,
                         int payloadBytes,
                         int durationSeconds,
                         int flushEvery,
                         int inFlightLimit,
                         AtomicLong published) {
            byte[] payload = new byte[payloadBytes];
            long endAt = System.nanoTime() + TimeUnit.SECONDS.toNanos(durationSeconds);
            int i = 0;
            long localSent = 0;
            while (System.nanoTime() < endAt) {
                if (!connection.channel().isActive()) {
                    break;
                }
                while ((localSent - localAcked.get()) >= inFlightLimit) {
                    submitFlush();
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
                while (!connection.channel().isWritable()) {
                    submitFlush();
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
                MqttFixedHeader fixedHeader = new MqttFixedHeader(
                        MqttMessageType.PUBLISH,
                        false,
                        MqttQoS.AT_LEAST_ONCE,
                        false,
                        0
                );
                int messageId = nextPacketId();
                MqttPublishVariableHeader header = new MqttPublishVariableHeader(topic, messageId);
                MqttPublishMessage message = new MqttPublishMessage(fixedHeader, header, Unpooled.wrappedBuffer(payload));
                submitWrite(message);
                if (flushEvery > 0 && (i + 1) % flushEvery == 0) {
                    submitFlush();
                }
                published.incrementAndGet();
                i++;
                localSent++;
            }
            submitFlush();
            return localSent;
        }

        /**
         * 等待 Ack 达到预期。
         *
         * @param acked          Ack 计数
         * @param expected       预期数量
         * @param timeoutSeconds 超时秒数
         */
        void awaitAcks(AtomicLong acked, long expected, int timeoutSeconds) {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
            while (System.nanoTime() < deadline) {
                if (localAcked.get() >= expected) {
                    return;
                }
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }

        /**
         * 发布一条消息并等待 Ack。
         *
         * @param topic          主题
         * @param payloadBytes   负载大小
         * @param timeoutSeconds 超时秒数
         * @return 是否收到 Ack
         */
        boolean publishOnceWaitAck(String topic, int payloadBytes, int timeoutSeconds) {
            byte[] payload = new byte[payloadBytes];
            MqttFixedHeader fixedHeader = new MqttFixedHeader(
                    MqttMessageType.PUBLISH,
                    false,
                    MqttQoS.AT_LEAST_ONCE,
                    false,
                    0
            );
            int messageId = nextPacketId();
            long beforeAck = localAcked.get();
            MqttPublishVariableHeader header = new MqttPublishVariableHeader(topic, messageId);
            MqttPublishMessage message = new MqttPublishMessage(fixedHeader, header, Unpooled.wrappedBuffer(payload));
            connection.channel().writeAndFlush(message).syncUninterruptibly();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
            while (System.nanoTime() < deadline) {
                if (localAcked.get() > beforeAck) {
                    return true;
                }
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
            return false;
        }

        /**
         * 关闭连接。
         */
        void close() {
            if (connection != null && !connection.isDisposed()) {
                connection.disposeNow();
            }
        }

        /**
         * 写入并刷新消息。
         *
         * @param message 消息
         */
        private void writeAndFlush(MqttMessage message) {
            if (connection.channel().eventLoop().inEventLoop()) {
                connection.channel().writeAndFlush(message).syncUninterruptibly();
                return;
            }
            connection.channel().eventLoop().submit(() -> connection.channel().writeAndFlush(message)).syncUninterruptibly();
        }

        /**
         * 提交写入操作。
         *
         * @param message 消息
         */
        private void submitWrite(MqttMessage message) {
            if (connection.channel().eventLoop().inEventLoop()) {
                connection.channel().write(message);
                return;
            }
            connection.channel().eventLoop().execute(() -> connection.channel().write(message));
        }

        /**
         * 提交刷新操作。
         */
        private void submitFlush() {
            if (connection.channel().eventLoop().inEventLoop()) {
                connection.channel().flush();
                return;
            }
            connection.channel().eventLoop().execute(() -> connection.channel().flush());
        }

        /**
         * 确保 MQTT 编解码器存在。
         */
        private void ensureMqttPipeline() {
            String bridgeName = "reactor.right.reactiveBridge";
            if (connection.channel().pipeline().get(bridgeName) != null) {
                if (connection.channel().pipeline().get(MqttEncoder.class) == null) {
                    connection.channel().pipeline().addBefore(bridgeName, "mqttEncoder", MqttEncoder.INSTANCE);
                }
                if (connection.channel().pipeline().get(MqttDecoder.class) == null) {
                    connection.channel().pipeline().addBefore(bridgeName, "mqttDecoder", new MqttDecoder(1024 * 1024));
                }
                return;
            }
            if (connection.channel().pipeline().get(MqttEncoder.class) == null) {
                connection.channel().pipeline().addFirst("mqttEncoder", MqttEncoder.INSTANCE);
            }
            if (connection.channel().pipeline().get(MqttDecoder.class) == null) {
                connection.channel().pipeline().addLast("mqttDecoder", new MqttDecoder(1024 * 1024));
            }
        }

        /**
         * 生成下一个消息 ID。
         *
         * @return 消息 ID
         */
        private int nextPacketId() {
            int id = packetId++;
            if (packetId > 0xFFFF) {
                packetId = 1;
            }
            return id;
        }

        /**
         * 处理入站消息。
         *
         * @param message 入站消息
         */
        private void onInbound(MqttMessage message) {
            if (message.fixedHeader().messageType() == MqttMessageType.PUBACK) {
                ackedCounter.incrementAndGet();
                localAcked.incrementAndGet();
                return;
            }
            inbox.add(message);
        }

        /**
         * 等待匹配的入站消息。
         *
         * @param predicate 过滤条件
         * @param timeout   超时时间
         * @return 匹配消息
         */
        private MqttMessage awaitMessage(java.util.function.Predicate<MqttMessage> predicate, Duration timeout) {
            long deadline = System.nanoTime() + timeout.toNanos();
            while (System.nanoTime() < deadline) {
                for (MqttMessage message : inbox) {
                    if (predicate.test(message)) {
                        inbox.remove(message);
                        return message;
                    }
                }
                try {
                    Thread.sleep(5);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return null;
                }
            }
            return null;
        }
    }

    private static final class StressConfig {
        private int port;
        private int threads;
        private int durationSeconds;
        private int payloadBytes;
        private int flushEvery;
        private int inFlightLimit;
        private int timeoutSeconds;
        private int reportIntervalSeconds;
        private String topic;
    }

    private static final class StressResult {
        private final long sent;
        private final long startNanos;
        private final long endNanos;
        private final boolean completed;

        /**
         * 构造压测结果。
         *
         * @param sent       发送数量
         * @param startNanos 开始时间
         * @param endNanos   结束时间
         * @param completed  是否完成
         */
        private StressResult(long sent, long startNanos, long endNanos, boolean completed) {
            this.sent = sent;
            this.startNanos = startNanos;
            this.endNanos = endNanos;
            this.completed = completed;
        }
    }

}