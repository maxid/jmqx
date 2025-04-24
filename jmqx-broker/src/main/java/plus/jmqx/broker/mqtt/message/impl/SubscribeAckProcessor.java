package plus.jmqx.broker.mqtt.message.impl;

import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import plus.jmqx.broker.mqtt.channel.MqttSession;
import plus.jmqx.broker.mqtt.message.MessageProcessor;
import plus.jmqx.broker.mqtt.message.MessageWrapper;
import reactor.util.context.ContextView;

import java.util.ArrayList;
import java.util.List;

/**
 * SUBACK 消息流程处理
 *
 * @author maxid
 * @since 2025/4/9 16:31
 */
public class SubscribeAckProcessor implements MessageProcessor<MqttSubAckMessage> {

    private static final List<MqttMessageType> MESSAGE_TYPES = new ArrayList<>();

    static {
        MESSAGE_TYPES.add(MqttMessageType.SUBACK);
    }

    @Override
    public List<MqttMessageType> getMqttMessageTypes() {
        return MESSAGE_TYPES;
    }

    @Override
    public void process(MessageWrapper<MqttSubAckMessage> wrapper, MqttSession session, ContextView view) {
        session.cancelRetry(MqttMessageType.SUBSCRIBE, wrapper.getMessage().variableHeader().messageId());
    }
}
