package plus.jmqx.broker.mqtt.message.impl;

import io.netty.handler.codec.mqtt.*;
import lombok.extern.slf4j.Slf4j;
import plus.jmqx.broker.cluster.ClusterSession;
import plus.jmqx.broker.config.Configuration;
import plus.jmqx.broker.mqtt.channel.MqttSession;
import plus.jmqx.broker.mqtt.context.ContextHolder;
import plus.jmqx.broker.mqtt.context.NamespaceContextHolder;
import plus.jmqx.broker.mqtt.context.ReceiveContext;
import plus.jmqx.broker.mqtt.message.MessageDispatcher;
import plus.jmqx.broker.mqtt.message.MessageProcessor;
import plus.jmqx.broker.mqtt.message.MessageWrapper;
import plus.jmqx.broker.spi.DynamicLoader;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.netty.ReactorNetty;
import reactor.util.context.Context;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Stream;

/**
 * MQTT 消息报文分发处理器
 *
 * @author maxid
 * @since 2025/4/15 09:35
 */
@Slf4j
public class MqttMessageDispatcher implements MessageDispatcher {

    private final Scheduler                                           publishScheduler;
    private final Scheduler                                           controlScheduler;
    private final Sinks.Many<MessageWrapper<MqttMessage>>             publishAcceptor;
    private final Sinks.Many<MessageWrapper<MqttMessage>>             controlAcceptor;
    private final Configuration                                       config;
    private final Map<MqttMessageType, MessageProcessor<MqttMessage>> processorMap;
    private final MessageProcessor<MqttMessage>                       defaultProcessor;

    /**
     * 构建分发器并初始化处理管线
     *
     * @param config     配置
     * @param threadSize 线程数
     * @param queueSize  队列大小
     */
    @SuppressWarnings("unchecked")
    public MqttMessageDispatcher(Configuration config, Integer threadSize, Integer queueSize) {
        this.config = config;
        int publishThreads = Math.max(1, threadSize - 1);
        int controlThreads = Math.max(1, threadSize - publishThreads);
        this.publishScheduler = Schedulers.newParallel("jmqx-publish-io", publishThreads);
        this.controlScheduler = Schedulers.newParallel("jmqx-control-io", controlThreads);
        this.publishAcceptor = Sinks.many().multicast().onBackpressureBuffer(queueSize);
        this.controlAcceptor = Sinks.many().multicast().onBackpressureBuffer(queueSize);
        Stream<MessageProcessor<?>> processors = (Stream<MessageProcessor<?>>) (Stream<?>) DynamicLoader.findAll(MessageProcessor.class);
        Map<MqttMessageType, MessageProcessor<MqttMessage>> map = new EnumMap<>(MqttMessageType.class);
        MessageProcessor<MqttMessage>[] commonHolder = new MessageProcessor[1];
        processors.forEach(p -> {
            p.setNamespace(config.getClusterConfig().getNamespace());
            if (p.getMessageType() == MessageProcessor.CommonMessageType.class) {
                commonHolder[0] = (MessageProcessor<MqttMessage>) p;
            }
            for (MqttMessageType type : p.getMqttMessageTypes()) {
                MessageProcessor<MqttMessage> previous = map.put(type, (MessageProcessor<MqttMessage>) p);
                if (previous != null && previous != p) {
                    log.warn("duplicate processor mapping for {}: {} -> {}", type, previous.getClass().getName(), p.getClass().getName());
                }
            }
        });
        this.processorMap = map;
        this.defaultProcessor = commonHolder[0];

        startConsumer(publishAcceptor, publishScheduler, publishThreads);
        startConsumer(controlAcceptor, controlScheduler, controlThreads);
    }

    /**
     * 按消息类型投递到对应处理通道
     *
     * @param session 会话
     * @param wrapper 消息包装
     * @param context 上下文
     * @param <C>     配置类型
     */
    @Override
    public <C extends Configuration> void dispatch(MqttSession session, MessageWrapper<MqttMessage> wrapper, ReceiveContext<C> context) {
        MqttMessage message = wrapper.getMessage();
        MqttMessageType messageType = message.fixedHeader().messageType();
        initSession(session, message, messageType);
        wrapper.setSession(session);
        if (messageType == MqttMessageType.PUBLISH) {
            this.publishAcceptor.emitNext(wrapper, RetryFailureHandler.RETRY_NON_SERIALIZED);
        } else {
            this.controlAcceptor.emitNext(wrapper, RetryFailureHandler.RETRY_NON_SERIALIZED);
        }
    }

    /**
     * 投递集群消息到本地分发器
     *
     * @param message 发布消息
     */
    @Override
    public void publish(MqttPublishMessage message) {
        ReceiveContext<?> context = contextHolder().getContext();
        if (context == null) {
            return;
        }
        MessageWrapper<MqttMessage> wrapper = new MessageWrapper<>(message, System.currentTimeMillis(), Boolean.TRUE);
        this.dispatch(ClusterSession.DEFAULT_CLUSTER_SESSION, wrapper, context);
    }

    /**
     * 初始化会话
     *
     * @param session     会话
     * @param mqttMessage Mqtt 消息
     * @param messageType Mqtt 消息类型
     */
    private void initSession(MqttSession session, MqttMessage mqttMessage, MqttMessageType messageType) {
        if (MqttMessageType.CONNECT.equals(messageType)) {
            MqttConnectMessage message = (MqttConnectMessage) mqttMessage;
            MqttConnectPayload payload = message.payload();
            String clientId = payload.clientIdentifier();
            String username = payload.userName();
            session.setClientId(clientId);
            session.setUsername(username);
        }
        if (MqttMessageType.CONNECT.equals(messageType) || MqttMessageType.DISCONNECT.equals(messageType)) {
            log.debug("【{}】{}", messageType, session);
        }
    }

    /**
     * 执行具体消息处理并释放资源
     *
     * @param wrapper 消息包装
     */
    private void processWrapper(MessageWrapper<MqttMessage> wrapper) {
        MqttMessage message = wrapper.getMessage();
        MqttMessageType messageType = message.fixedHeader().messageType();
        MessageProcessor<MqttMessage> processor = processorMap.get(messageType);
        if (processor == null) {
            processor = defaultProcessor;
            if (processor == null) {
                log.warn("no MessageProcessor for messageType {}", messageType);
                ReactorNetty.safeRelease(message.payload());
                return;
            }
        }
        ReceiveContext<?> context = contextHolder().getContext();
        if (context == null) {
            ReactorNetty.safeRelease(message.payload());
            return;
        }
        try {
            processor.process(wrapper, wrapper.getSession(), Context.of(ReceiveContext.class, context));
        } catch (Exception e) {
            log.error("session {}, message: {}, error: {}", wrapper.getSession(), message, e.getMessage());
        } finally {
            ReactorNetty.safeRelease(message.payload());
        }
    }

    /**
     * 启动消费管线
     *
     * @param sink        消息接收器
     * @param scheduler   调度器
     * @param concurrency 并发度
     */
    private void startConsumer(Sinks.Many<MessageWrapper<MqttMessage>> sink, Scheduler scheduler, int concurrency) {
        int parallelism = Math.max(1, concurrency);
        sink.asFlux()
                .doOnError(e -> log.error("MqttMessageDispatcher consumer", e))
                .onErrorResume(e -> Mono.empty())
                .publishOn(scheduler)
                .flatMap(wrapper -> Mono.fromRunnable(() -> processWrapper(wrapper)), parallelism, parallelism)
                .subscribe();
    }

    /**
     * 失败重试处理器
     */
    public static class RetryFailureHandler implements Sinks.EmitFailureHandler {

        public static final RetryFailureHandler RETRY_NON_SERIALIZED = new RetryFailureHandler();

        /**
         * 创建失败重试处理器
         */
        public RetryFailureHandler() {
        }

        /**
         * 失败时短暂退避并重试
         *
         * @param signalType 信号类型
         * @param emitResult 发送结果
         * @return 是否继续重试
         */
        @Override
        public boolean onEmitFailure(SignalType signalType, Sinks.EmitResult emitResult) {
            LockSupport.parkNanos(10);
            return emitResult == Sinks.EmitResult.FAIL_NON_SERIALIZED;
        }
    }

    /**
     * 获取上下文持有器
     *
     * @return 上下文持有器
     */
    private ContextHolder contextHolder() {
        return NamespaceContextHolder.get(config.getClusterConfig().getNamespace());
    }

}
