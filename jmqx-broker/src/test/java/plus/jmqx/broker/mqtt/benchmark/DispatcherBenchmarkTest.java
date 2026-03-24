package plus.jmqx.broker.mqtt.benchmark;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.junit.jupiter.api.Test;
import plus.jmqx.broker.config.Configuration;
import plus.jmqx.broker.mqtt.channel.MqttSession;
import plus.jmqx.broker.mqtt.context.ReceiveContext;
import plus.jmqx.broker.mqtt.message.MessageProcessor;
import plus.jmqx.broker.mqtt.message.MessageWrapper;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.netty.ReactorNetty;
import reactor.util.context.Context;
import reactor.util.context.ContextView;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * 简易对比基准：Legacy vs Optimized dispatch pipeline。
 */
class DispatcherBenchmarkTest {

    /**
     * 对比 Legacy 与 Optimized 调度性能。
     *
     * @throws Exception 测试异常
     */
    @Test
    void compareLegacyAndOptimized() throws Exception {
        int threadSize = Math.max(1, Runtime.getRuntime().availableProcessors());
        int messageCount = 20_000_000;
        int payloadSize = 16;
        int queueSize = Math.max(1024, messageCount);

        BenchmarkResult legacy = runBenchmark("legacy", new LegacyPipeline(threadSize, queueSize), messageCount, payloadSize);
        BenchmarkResult optimized = runBenchmark("optimized", new OptimizedPipeline(threadSize, queueSize), messageCount, payloadSize);

        System.out.println("Benchmark results (messages=" + messageCount + ", payload=" + payloadSize + "B):");
        System.out.println("legacy:    " + legacy.format());
        System.out.println("optimized: " + optimized.format());
    }

    /**
     * 执行基准测试。
     *
     * @param name         基准名称
     * @param pipeline     调度管道
     * @param messageCount 消息数量
     * @param payloadSize  负载大小
     * @return 基准结果
     * @throws Exception 测试异常
     */
    private BenchmarkResult runBenchmark(String name, Pipeline pipeline, int messageCount, int payloadSize) throws Exception {
        CountDownLatch latch = new CountDownLatch(messageCount);
        NoopProcessor processor = new NoopProcessor(latch);
        ReceiveContext<Configuration> context = new NoopReceiveContext();
        pipeline.start(processor, context);

        long start = System.nanoTime();
        for (int i = 0; i < messageCount; i++) {
            MqttPublishMessage message = newPublishMessage(payloadSize);
            MessageWrapper<MqttPublishMessage> wrapper = new MessageWrapper<>(message, System.currentTimeMillis(), Boolean.FALSE);
            wrapper.setSession(new MqttSession());
            pipeline.emit(wrapper);
        }

        boolean ok = latch.await(60, TimeUnit.SECONDS);
        long end = System.nanoTime();
        pipeline.close();

        if (!ok) {
            throw new IllegalStateException(name + " pipeline did not finish in time");
        }
        return BenchmarkResult.from(name, messageCount, start, end);
    }

    /**
     * 构造发布消息。
     *
     * @param payloadSize 负载大小
     * @return 发布消息
     */
    private MqttPublishMessage newPublishMessage(int payloadSize) {
        MqttFixedHeader fixedHeader = new MqttFixedHeader(
                MqttMessageType.PUBLISH,
                false,
                MqttQoS.AT_MOST_ONCE,
                false,
                0
        );
        MqttPublishVariableHeader variableHeader = new MqttPublishVariableHeader("bench/topic", 1);
        return new MqttPublishMessage(fixedHeader, variableHeader, Unpooled.buffer(payloadSize));
    }

    private interface Pipeline extends AutoCloseable {
        /**
         * 启动管道。
         *
         * @param processor 处理器
         * @param context   上下文
         */
        void start(MessageProcessor<MqttPublishMessage> processor, ReceiveContext<Configuration> context);
        /**
         * 发送消息包装。
         *
         * @param wrapper 消息包装
         */
        void emit(MessageWrapper<MqttPublishMessage> wrapper);
        @Override
        /**
         * 关闭管道。
         */
        void close();
    }

    private static final class LegacyPipeline implements Pipeline {
        private final Scheduler scheduler;
        private final Sinks.Many<MessageWrapper<MqttPublishMessage>> sink;
        private final EmitFailureHandler emitFailureHandler = new EmitFailureHandler();

        /**
         * 构造 Legacy 管道。
         *
         * @param threadSize 线程数
         * @param queueSize  队列大小
         */
        LegacyPipeline(int threadSize, int queueSize) {
            this.scheduler = Schedulers.newParallel("legacy-dispatch", threadSize);
            this.sink = Sinks.many().multicast().onBackpressureBuffer(queueSize);
        }

        /**
         * 启动管道并订阅处理。
         *
         * @param processor 处理器
         * @param context   上下文
         */
        @Override
        public void start(MessageProcessor<MqttPublishMessage> processor, ReceiveContext<Configuration> context) {
            sink.asFlux()
                    .publishOn(scheduler)
                    .subscribe(wrapper -> {
                        MqttMessage message = wrapper.getMessage();
                        processor.process(wrapper, wrapper.getSession())
                                .contextWrite(Context.of(ReceiveContext.class, context))
                                .onErrorContinue((e, o) -> {})
                                .subscribe(v -> {},
                                        e -> ReactorNetty.safeRelease(message.payload()),
                                        () -> ReactorNetty.safeRelease(message.payload()));
                    });
        }

        /**
         * 发送消息包装。
         *
         * @param wrapper 消息包装
         */
        @Override
        public void emit(MessageWrapper<MqttPublishMessage> wrapper) {
            sink.emitNext(wrapper, emitFailureHandler);
        }

        /**
         * 关闭管道。
         */
        @Override
        public void close() {
            scheduler.dispose();
        }
    }

    private static final class OptimizedPipeline implements Pipeline {
        private final Scheduler scheduler;
        private final Sinks.Many<MessageWrapper<MqttPublishMessage>> sink;
        private final EmitFailureHandler emitFailureHandler = new EmitFailureHandler();
        private final int concurrency;

        /**
         * 构造 Optimized 管道。
         *
         * @param threadSize 线程数
         * @param queueSize  队列大小
         */
        OptimizedPipeline(int threadSize, int queueSize) {
            this.scheduler = Schedulers.newParallel("optimized-dispatch", threadSize);
            this.sink = Sinks.many().multicast().onBackpressureBuffer(queueSize);
            this.concurrency = Math.max(1, threadSize);
        }

        /**
         * 启动管道并订阅处理。
         *
         * @param processor 处理器
         * @param context   上下文
         */
        @Override
        public void start(MessageProcessor<MqttPublishMessage> processor, ReceiveContext<Configuration> context) {
            sink.asFlux()
                    .publishOn(scheduler)
                    .flatMap(wrapper -> Mono.fromRunnable(() -> {
                        try {
                            processor.process(wrapper, wrapper.getSession(),
                                    Context.of(ReceiveContext.class, context));
                        } finally {
                            ReactorNetty.safeRelease(wrapper.getMessage().payload());
                        }
                    }), concurrency, concurrency)
                    .subscribe();
        }

        /**
         * 发送消息包装。
         *
         * @param wrapper 消息包装
         */
        @Override
        public void emit(MessageWrapper<MqttPublishMessage> wrapper) {
            sink.emitNext(wrapper, emitFailureHandler);
        }

        /**
         * 关闭管道。
         */
        @Override
        public void close() {
            scheduler.dispose();
        }
    }

    private static final class EmitFailureHandler implements Sinks.EmitFailureHandler {
        /**
         * 处理发送失败。
         *
         * @param signalType  信号类型
         * @param emitResult  发送结果
         * @return 是否重试
         */
        @Override
        public boolean onEmitFailure(SignalType signalType, Sinks.EmitResult emitResult) {
            LockSupport.parkNanos(10);
            return emitResult == Sinks.EmitResult.FAIL_NON_SERIALIZED;
        }
    }

    private static final class NoopProcessor implements MessageProcessor<MqttPublishMessage> {
        private final CountDownLatch latch;
        private final AtomicLong counter = new AtomicLong();

        /**
         * 构造空处理器。
         *
         * @param latch 计数器
         */
        private NoopProcessor(CountDownLatch latch) {
            this.latch = latch;
        }

        /**
         * 获取命名空间。
         *
         * @return 命名空间
         */
        @Override
        public String getNamespace() {
            return "bench";
        }

        /**
         * 设置命名空间。
         *
         * @param namespace 命名空间
         */
        @Override
        public void setNamespace(String namespace) {}

        /**
         * 获取处理的消息类型集合。
         *
         * @return 消息类型列表
         */
        @Override
        public List<MqttMessageType> getMqttMessageTypes() {
            return Arrays.asList(MqttMessageType.PUBLISH);
        }

        /**
         * 获取处理器类型。
         *
         * @return 处理器类型
         */
        @Override
        public Class<?> getMessageType() {
            return MessageProcessor.PublishMessageType.class;
        }

        /**
         * 处理消息包装。
         *
         * @param wrapper 消息包装
         * @param session 会话
         * @param view    上下文视图
         */
        @Override
        public void process(MessageWrapper<MqttPublishMessage> wrapper, MqttSession session, ContextView view) {
            counter.incrementAndGet();
            latch.countDown();
        }
    }

    private static final class NoopReceiveContext implements ReceiveContext<Configuration> {
        /**
         * 获取配置。
         *
         * @return 配置
         */
        @Override
        public Configuration getConfiguration() { return null; }
        /**
         * 获取 ACK 管理器。
         *
         * @return ACK 管理器
         */
        @Override
        public plus.jmqx.broker.mqtt.retry.TimeAckManager getTimeAckManager() { return null; }
        /**
         * 获取集群注册中心。
         *
         * @return 集群注册中心
         */
        @Override
        public plus.jmqx.broker.cluster.ClusterRegistry getClusterRegistry() { return null; }
        /**
         * 获取事件注册中心。
         *
         * @return 事件注册中心
         */
        @Override
        public plus.jmqx.broker.mqtt.registry.EventRegistry getEventRegistry() { return null; }
        /**
         * 获取会话注册中心。
         *
         * @return 会话注册中心
         */
        @Override
        public plus.jmqx.broker.mqtt.registry.SessionRegistry getSessionRegistry() { return null; }
        /**
         * 获取主题注册中心。
         *
         * @return 主题注册中心
         */
        @Override
        public plus.jmqx.broker.mqtt.registry.TopicRegistry getTopicRegistry() { return null; }
        /**
         * 获取消息注册中心。
         *
         * @return 消息注册中心
         */
        @Override
        public plus.jmqx.broker.mqtt.registry.MessageRegistry getMessageRegistry() { return null; }
        /**
         * 获取消息分发器。
         *
         * @return 消息分发器
         */
        @Override
        public plus.jmqx.broker.mqtt.message.MessageDispatcher getMessageDispatcher() { return null; }
        /**
         * 获取 ACL 管理器。
         *
         * @return ACL 管理器
         */
        @Override
        public plus.jmqx.broker.acl.AclManager getAclManager() { return null; }
        /**
         * 获取鉴权管理器。
         *
         * @return 鉴权管理器
         */
        @Override
        public plus.jmqx.broker.auth.AuthManager getAuthManager() { return null; }
        /**
         * 调用分发器。
         *
         * @param consumer 分发器消费者
         */
        @Override
        public void dispatch(java.util.function.Consumer<plus.jmqx.broker.mqtt.message.dispatch.PlatformDispatcher> consumer) {}
        /**
         * 接收消息。
         *
         * @param session 会话
         * @param wrapper 消息包装
         */
        @Override
        public void accept(MqttSession session, MessageWrapper<MqttMessage> wrapper) {}
    }

    private static final class BenchmarkResult {
        private final String name;
        private final int count;
        private final long startNs;
        private final long endNs;

        /**
         * 构造基准结果。
         *
         * @param name    名称
         * @param count   数量
         * @param startNs 开始时间
         * @param endNs   结束时间
         */
        private BenchmarkResult(String name, int count, long startNs, long endNs) {
            this.name = name;
            this.count = count;
            this.startNs = startNs;
            this.endNs = endNs;
        }

        /**
         * 构造结果对象。
         *
         * @param name    名称
         * @param count   数量
         * @param startNs 开始时间
         * @param endNs   结束时间
         * @return 结果
         */
        static BenchmarkResult from(String name, int count, long startNs, long endNs) {
            return new BenchmarkResult(name, count, startNs, endNs);
        }

        /**
         * 输出格式化结果。
         *
         * @return 格式化字符串
         */
        String format() {
            double seconds = (endNs - startNs) / 1_000_000_000.0;
            double qps = count / seconds;
            return String.format("%s: time=%.3fs, throughput=%.0f msg/s", name, seconds, qps);
        }
    }
}
