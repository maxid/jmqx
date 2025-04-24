package plus.jmqx.broker.mqtt.message.impl;

import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import lombok.extern.slf4j.Slf4j;
import plus.jmqx.broker.config.Configuration;
import plus.jmqx.broker.mqtt.channel.MqttSession;
import plus.jmqx.broker.mqtt.context.ReceiveContext;
import plus.jmqx.broker.mqtt.message.MessageDispatcher;
import plus.jmqx.broker.mqtt.message.MessageProcessor;
import plus.jmqx.broker.mqtt.message.MessageWrapper;
import plus.jmqx.broker.spi.DynamicLoader;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.netty.ReactorNetty;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * MQTT 消息报文分发处理器
 *
 * @author maxid
 * @since 2025/4/15 09:35
 */
@Slf4j
public class MqttMessageDispatcher implements MessageDispatcher {
    private final Map<MqttMessageType, MessageProcessor<MqttMessage>> types = new HashMap<>();

    private final Scheduler scheduler;

    @SuppressWarnings("unchecked")
    public MqttMessageDispatcher(Scheduler scheduler) {
        this.scheduler = Optional.ofNullable(scheduler).orElse(Schedulers.boundedElastic());
        DynamicLoader.findAll(MessageProcessor.class)
                .forEach(processor -> processor.getMqttMessageTypes().forEach(o -> {
                    MqttMessageType type = (MqttMessageType) o;
                    types.put(type, processor);
                }));
    }

    @Override
    public <C extends Configuration> void dispatch(MqttSession session, MessageWrapper<MqttMessage> wrapper, ReceiveContext<C> context) {
        MqttMessage message = wrapper.getMessage();
        log.info("【{}】{}",message.fixedHeader().messageType(), session);
        Optional.ofNullable(types.get(message.fixedHeader().messageType()))
                .ifPresent(processor -> processor
                        .process(wrapper, session)
                        .contextWrite(view -> view.putNonNull(ReceiveContext.class, context))
                        .subscribeOn(this.scheduler)
                        .subscribe(v -> {}, err -> {
                            log.error("session {}, message: {}, error: {}", session, message, err.getMessage());
                            ReactorNetty.safeRelease(message.payload());
                        }, () -> ReactorNetty.safeRelease(message.payload())));
    }
}
