package plus.jmqx.broker.mqtt.message.impl;

import io.netty.handler.codec.mqtt.*;
import lombok.extern.slf4j.Slf4j;
import plus.jmqx.broker.cluster.ClusterSession;
import plus.jmqx.broker.config.Configuration;
import plus.jmqx.broker.mqtt.channel.MqttSession;
import plus.jmqx.broker.mqtt.context.ContextHolder;
import plus.jmqx.broker.mqtt.context.ReceiveContext;
import plus.jmqx.broker.mqtt.message.MessageDispatcher;
import plus.jmqx.broker.mqtt.message.MessageProcessor;
import plus.jmqx.broker.mqtt.message.MessageTypeWrapper;
import plus.jmqx.broker.mqtt.message.MessageWrapper;
import plus.jmqx.broker.spi.DynamicLoader;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.netty.ReactorNetty;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.LockSupport;

/**
 * MQTT 消息报文分发处理器
 *
 * @author maxid
 * @since 2025/4/15 09:35
 */
@Slf4j
public class MqttMessageDispatcher implements MessageDispatcher {

    private final Scheduler                                   scheduler;
    private final Sinks.Many<MessageTypeWrapper<MqttMessage>> acceptor;

    @SuppressWarnings("unchecked")
    public MqttMessageDispatcher(Integer threadSize, Integer queueSize) {
        this.scheduler = Schedulers.newParallel("jmqx-acceptor-io", threadSize);
        this.acceptor = Sinks.many().multicast().onBackpressureBuffer(queueSize);
        DynamicLoader.findAll(MessageProcessor.class)
                .forEach(p -> acceptor.asFlux()
                        .doOnError(e -> log.error("MqttMessageDispatcher consumer", e))
                        .onErrorResume(e -> Mono.empty())
                        .ofType(p.getMessageType())
                        .publishOn(scheduler)
                        .subscribe(type -> {
                            MessageWrapper<MqttMessage> wrapper = ((MessageTypeWrapper<MqttMessage>) type).getWrapper();
                            MessageProcessor<MqttMessage> processor = (MessageProcessor<MqttMessage>) p;
                            MqttSession session = wrapper.getSession();
                            MqttMessage message = wrapper.getMessage();
                            processor.process(wrapper, session)
                                    .contextWrite(view -> view.putNonNull(ReceiveContext.class, ContextHolder.getContext()))
                                    .onErrorContinue((e, o) -> log.error("MqttMessageDispatcher", e))
                                    // TODO 待性能优化
                                    .subscribe(v -> {
                                    }, e -> {
                                        log.error("session {}, message: {}, error: {}", session, message, e.getMessage());
                                        ReactorNetty.safeRelease(message.payload());
                                    }, () -> ReactorNetty.safeRelease(message.payload()));
                        }));
    }

    @Override
    public <C extends Configuration> void dispatch(MqttSession session, MessageWrapper<MqttMessage> wrapper, ReceiveContext<C> context) {
        MqttMessage message = wrapper.getMessage();
        MqttMessageType messageType = message.fixedHeader().messageType();
        initSession(session, message, messageType);
        wrapper.setSession(session);
        this.acceptor.emitNext(wrapper(wrapper), RetryFailureHandler.RETRY_NON_SERIALIZED);
    }

    @Override
    public void publish(MqttPublishMessage message) {
        ReceiveContext<?> context = ContextHolder.getContext();
        if (context == null) {
            return;
        }
        MessageWrapper<MqttMessage> wrapper = new MessageWrapper<>(message, System.currentTimeMillis(), Boolean.TRUE);
        this.dispatch(ClusterSession.DEFAULT_CLUSTER_SESSION, wrapper, context);
    }

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

    private MessageTypeWrapper<MqttMessage> wrapper(MessageWrapper<MqttMessage> wrapper) {
        MqttMessageType messageType = wrapper.getMessage().fixedHeader().messageType();
        switch (messageType) {
            case CONNECT:
                return (MessageTypeWrapper) MessageProcessor.ConnectMessageType.of(wrapper);
            case PUBACK:
                return (MessageTypeWrapper) MessageProcessor.PublishAckMessageType.of(wrapper);
            case PUBLISH:
                return (MessageTypeWrapper) MessageProcessor.PublishMessageType.of(wrapper);
            case SUBACK:
                return (MessageTypeWrapper) MessageProcessor.SubscribeAckMessageType.of(wrapper);
            case SUBSCRIBE:
                return (MessageTypeWrapper) MessageProcessor.SubscribeMessageType.of(wrapper);
            case UNSUBACK:
                return (MessageTypeWrapper) MessageProcessor.UnsubscribeAckMessageType.of(wrapper);
            case UNSUBSCRIBE:
                return (MessageTypeWrapper) MessageProcessor.UnsubscribeMessageType.of(wrapper);
            case PINGRESP:
            case PINGREQ:
            case DISCONNECT:
            case PUBCOMP:
            case PUBREC:
            case PUBREL:
            default:
                return MessageProcessor.CommonMessageType.of(wrapper);
        }
    }

    public static class RetryFailureHandler implements Sinks.EmitFailureHandler {

        public static final RetryFailureHandler RETRY_NON_SERIALIZED = new RetryFailureHandler();

        public RetryFailureHandler() {
        }

        @Override
        public boolean onEmitFailure(SignalType signalType, Sinks.EmitResult emitResult) {
            LockSupport.parkNanos(10);
            return emitResult == Sinks.EmitResult.FAIL_NON_SERIALIZED;
        }
    }
}
