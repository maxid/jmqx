package plus.jmqx.broker.mqtt.message.impl;

import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttUnsubAckMessage;
import plus.jmqx.broker.mqtt.channel.MqttSession;
import plus.jmqx.broker.mqtt.message.MessageProcessor;
import plus.jmqx.broker.mqtt.message.MessageWrapper;
import reactor.core.publisher.Mono;
import reactor.util.context.ContextView;

import java.util.ArrayList;
import java.util.List;

/**
 * UNSUBACK 消息流程处理
 *
 * @author maxid
 * @since 2025/4/9 16:33
 */
public class UnsubscribeAckProcessor implements MessageProcessor<MqttUnsubAckMessage> {

    private static final List<MqttMessageType> MESSAGE_TYPES = new ArrayList<>();

    static {
        MESSAGE_TYPES.add(MqttMessageType.UNSUBACK);
    }

    @Override
    public List<MqttMessageType> getMqttMessageTypes() {
        return MESSAGE_TYPES;
    }

    @Override
    public Mono<Void> process(MessageWrapper<MqttUnsubAckMessage> wrapper, MqttSession session, ContextView view) {
        return session.cancelRetry(MqttMessageType.UNSUBSCRIBE, wrapper.getMessage().variableHeader().messageId());
    }
}
