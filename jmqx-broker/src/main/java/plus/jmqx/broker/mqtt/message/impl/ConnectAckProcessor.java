package plus.jmqx.broker.mqtt.message.impl;

import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import plus.jmqx.broker.mqtt.channel.MqttSession;
import plus.jmqx.broker.mqtt.message.MessageProcessor;
import plus.jmqx.broker.mqtt.message.MessageWrapper;
import reactor.util.context.ContextView;

import java.util.ArrayList;
import java.util.List;

/**
 * CONNACK 消息流程处理
 *
 * @author maxid
 * @since 2025/4/9 16:29
 */
public class ConnectAckProcessor implements MessageProcessor<MqttConnAckMessage> {

    private static final List<MqttMessageType> MESSAGE_TYPES = new ArrayList<>();

    static {
        MESSAGE_TYPES.add(MqttMessageType.CONNACK);
    }

    @Override
    public List<MqttMessageType> getMqttMessageTypes() {
        return MESSAGE_TYPES;
    }

    @Override
    public void process(MessageWrapper<MqttConnAckMessage> wrapper, MqttSession session, ContextView view) {
        session.cancelRetry(MqttMessageType.CONNECT, -1);
    }
}
