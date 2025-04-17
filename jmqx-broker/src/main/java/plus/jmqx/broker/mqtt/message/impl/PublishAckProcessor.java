package plus.jmqx.broker.mqtt.message.impl;

import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPubAckMessage;
import plus.jmqx.broker.mqtt.channel.MqttChannel;
import plus.jmqx.broker.mqtt.context.ReceiveContext;
import plus.jmqx.broker.mqtt.message.MessageProcessor;
import plus.jmqx.broker.mqtt.message.MessageWrapper;
import plus.jmqx.broker.mqtt.retry.Ack;
import reactor.core.publisher.Mono;
import reactor.util.context.ContextView;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * 尽量简洁一句描述
 *
 * @author maxid
 * @since 2025/4/9 16:30
 */
public class PublishAckProcessor implements MessageProcessor<MqttPubAckMessage> {

    private static final List<MqttMessageType> MESSAGE_TYPES = new ArrayList<>();

    static {
        MESSAGE_TYPES.add(MqttMessageType.PUBACK);
    }

    @Override
    public List<MqttMessageType> getMqttMessageTypes() {
        return MESSAGE_TYPES;
    }

    @Override
    public Mono<Void> process(MessageWrapper<MqttPubAckMessage> message, MqttChannel session, ContextView view) {
        ReceiveContext<?> context = view.get(ReceiveContext.class);
        MqttPubAckMessage mqttMessage = message.getMessage();
        MqttMessageIdVariableHeader header = mqttMessage.variableHeader();
        int messageId = header.messageId();
        return Mono.fromRunnable(() -> {
            Ack ack = context.getTimeAckManager().getAck(session.generateId(MqttMessageType.PUBLISH, messageId));
            Optional.ofNullable(ack).ifPresent(Ack::stop);
        });
    }
}
