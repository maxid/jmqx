package plus.jmqx.broker.mqtt.message.impl;

import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPubAckMessage;
import plus.jmqx.broker.mqtt.channel.MqttSession;
import plus.jmqx.broker.mqtt.context.ReceiveContext;
import plus.jmqx.broker.mqtt.message.MessageWrapper;
import plus.jmqx.broker.mqtt.message.NamespceMessageProcessor;
import plus.jmqx.broker.mqtt.retry.Ack;
import reactor.util.context.ContextView;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * PUBACK 消息流程处理
 *
 * @author maxid
 * @since 2025/4/9 16:30
 */
public class PublishAckProcessor extends NamespceMessageProcessor<MqttPubAckMessage> {

    private static final List<MqttMessageType> MESSAGE_TYPES = new ArrayList<>();

    static {
        MESSAGE_TYPES.add(MqttMessageType.PUBACK);
    }

    @Override
    public List<MqttMessageType> getMqttMessageTypes() {
        return MESSAGE_TYPES;
    }

    @Override
    public Class<PublishAckMessageType> getMessageType() {
        return PublishAckMessageType.class;
    }

    @Override
    public void process(MessageWrapper<MqttPubAckMessage> wrapper, MqttSession session, ContextView view) {
        ReceiveContext<?> context = view.get(ReceiveContext.class);
        MqttPubAckMessage message = wrapper.getMessage();
        MqttMessageIdVariableHeader header = message.variableHeader();
        int messageId = header.messageId();
        Ack ack = context.getTimeAckManager().getAck(session.generateId(MqttMessageType.PUBLISH, messageId));
        Optional.ofNullable(ack).ifPresent(Ack::stop);
    }
}
