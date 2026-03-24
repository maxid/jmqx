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

    /**
     * 返回处理的消息类型列表。
     *
     * @return 消息类型列表
     */
    @Override
    public List<MqttMessageType> getMqttMessageTypes() {
        return MESSAGE_TYPES;
    }

    /**
     * 返回发布确认消息类型包装。
     *
     * @return 发布确认消息类型包装类
     */
    @Override
    public Class<PublishAckMessageType> getMessageType() {
        return PublishAckMessageType.class;
    }

    /**
     * 处理发布确认消息并停止重试。
     *
     * @param wrapper 消息包装
     * @param session 会话
     * @param view    上下文视图
     */
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
