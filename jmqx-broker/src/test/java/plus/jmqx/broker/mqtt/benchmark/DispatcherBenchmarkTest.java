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

    @Test
    void compareLegacyAndOptimized() throws Exception {
        int threadSize = Math.max(1, Runtime.getRuntime().availableProcessors());
        int messageCount = 200_000;
        int payloadSize = 16;
        int queueSize = Math.max(1024, messageCount);

        BenchmarkResult legacy = runBenchmark("legacy", new LegacyPipeline(threadSize, queueSize), messageCount, payloadSize);
        BenchmarkResult optimized = runBenchmark("optimized", new OptimizedPipeline(threadSize, queueSize), messageCount, payloadSize);

        System.out.println("Benchmark results (messages=" + messageCount + ", payload=" + payloadSize + "B):");
        System.out.println("legacy:    " + legacy.format());
        System.out.println("optimized: " + optimized.format());
    }

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
        void start(MessageProcessor<MqttPublishMessage> processor, ReceiveContext<Configuration> context);
        void emit(MessageWrapper<MqttPublishMessage> wrapper);
        @Override
        void close();
    }

    private static final class LegacyPipeline implements Pipeline {
        private final Scheduler scheduler;
        private final Sinks.Many<MessageWrapper<MqttPublishMessage>> sink;
        private final EmitFailureHandler emitFailureHandler = new EmitFailureHandler();

        LegacyPipeline(int threadSize, int queueSize) {
            this.scheduler = Schedulers.newParallel("legacy-dispatch", threadSize);
            this.sink = Sinks.many().multicast().onBackpressureBuffer(queueSize);
        }

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

        @Override
        public void emit(MessageWrapper<MqttPublishMessage> wrapper) {
            sink.emitNext(wrapper, emitFailureHandler);
        }

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

        OptimizedPipeline(int threadSize, int queueSize) {
            this.scheduler = Schedulers.newParallel("optimized-dispatch", threadSize);
            this.sink = Sinks.many().multicast().onBackpressureBuffer(queueSize);
            this.concurrency = Math.max(1, threadSize);
        }

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

        @Override
        public void emit(MessageWrapper<MqttPublishMessage> wrapper) {
            sink.emitNext(wrapper, emitFailureHandler);
        }

        @Override
        public void close() {
            scheduler.dispose();
        }
    }

    private static final class EmitFailureHandler implements Sinks.EmitFailureHandler {
        @Override
        public boolean onEmitFailure(SignalType signalType, Sinks.EmitResult emitResult) {
            LockSupport.parkNanos(10);
            return emitResult == Sinks.EmitResult.FAIL_NON_SERIALIZED;
        }
    }

    private static final class NoopProcessor implements MessageProcessor<MqttPublishMessage> {
        private final CountDownLatch latch;
        private final AtomicLong counter = new AtomicLong();

        private NoopProcessor(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public String getNamespace() {
            return "bench";
        }

        @Override
        public void setNamespace(String namespace) {}

        @Override
        public List<MqttMessageType> getMqttMessageTypes() {
            return Arrays.asList(MqttMessageType.PUBLISH);
        }

        @Override
        public Class<?> getMessageType() {
            return MessageProcessor.PublishMessageType.class;
        }

        @Override
        public void process(MessageWrapper<MqttPublishMessage> wrapper, MqttSession session, ContextView view) {
            counter.incrementAndGet();
            latch.countDown();
        }
    }

    private static final class NoopReceiveContext implements ReceiveContext<Configuration> {
        @Override
        public Configuration getConfiguration() { return null; }
        @Override
        public plus.jmqx.broker.mqtt.retry.TimeAckManager getTimeAckManager() { return null; }
        @Override
        public plus.jmqx.broker.cluster.ClusterRegistry getClusterRegistry() { return null; }
        @Override
        public plus.jmqx.broker.mqtt.registry.EventRegistry getEventRegistry() { return null; }
        @Override
        public plus.jmqx.broker.mqtt.registry.SessionRegistry getSessionRegistry() { return null; }
        @Override
        public plus.jmqx.broker.mqtt.registry.TopicRegistry getTopicRegistry() { return null; }
        @Override
        public plus.jmqx.broker.mqtt.registry.MessageRegistry getMessageRegistry() { return null; }
        @Override
        public plus.jmqx.broker.mqtt.message.MessageDispatcher getMessageDispatcher() { return null; }
        @Override
        public plus.jmqx.broker.acl.AclManager getAclManager() { return null; }
        @Override
        public plus.jmqx.broker.auth.AuthManager getAuthManager() { return null; }
        @Override
        public void dispatch(java.util.function.Consumer<plus.jmqx.broker.mqtt.message.dispatch.PlatformDispatcher> consumer) {}
        @Override
        public void accept(MqttSession session, MessageWrapper<MqttMessage> wrapper) {}
    }

    private static final class BenchmarkResult {
        private final String name;
        private final int count;
        private final long startNs;
        private final long endNs;

        private BenchmarkResult(String name, int count, long startNs, long endNs) {
            this.name = name;
            this.count = count;
            this.startNs = startNs;
            this.endNs = endNs;
        }

        static BenchmarkResult from(String name, int count, long startNs, long endNs) {
            return new BenchmarkResult(name, count, startNs, endNs);
        }

        String format() {
            double seconds = (endNs - startNs) / 1_000_000_000.0;
            double qps = count / seconds;
            return String.format("%s: time=%.3fs, throughput=%.0f msg/s", name, seconds, qps);
        }
    }
}
