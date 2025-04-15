package plus.jmqx.broker.mqtt.message.impl;

import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import lombok.extern.slf4j.Slf4j;
import plus.jmqx.broker.config.Configuration;
import plus.jmqx.broker.mqtt.channel.MqttChannel;
import plus.jmqx.broker.mqtt.context.ReceiveContext;
import plus.jmqx.broker.mqtt.message.MessageAdapter;
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
 * MQTT 消息分发适配器
 *
 * @author maxid
 * @since 2025/4/15 09:35
 */
@Slf4j
public class MqttMessageAdapter implements MessageAdapter {
    private Map<MqttMessageType, MessageProcessor<MqttMessage>> types = new HashMap<>();

    private final Scheduler scheduler;

    @SuppressWarnings("unchecked")
    public MqttMessageAdapter(Scheduler scheduler) {
        this.scheduler = Optional.ofNullable(scheduler).orElse(Schedulers.boundedElastic());
        DynamicLoader.findAll(MessageProcessor.class)
                .forEach(processor -> processor.getMqttMessageTypes().forEach(o -> {
                    MqttMessageType type = (MqttMessageType) o;
                    types.put(type, processor);
                }));
    }

    @Override
    public <C extends Configuration> void dispatch(MqttChannel session, MessageWrapper<MqttMessage> message, ReceiveContext<C> context) {
        MqttMessage mqttMessage = message.getMessage();
        Optional.ofNullable(types.get(mqttMessage.fixedHeader().messageType()))
                .ifPresent(processor -> processor
                        .process(message, session)
                        .contextWrite(view -> view.putNonNull(ReceiveContext.class, context))
                        .subscribeOn(this.scheduler)
                        .subscribe(v -> {}, err -> {
                            log.error("session {}, message: {}, error: {}", session, mqttMessage, err.getMessage());
                            ReactorNetty.safeRelease(mqttMessage.payload());
                        }, () -> ReactorNetty.safeRelease(mqttMessage.payload())));
    }
}
