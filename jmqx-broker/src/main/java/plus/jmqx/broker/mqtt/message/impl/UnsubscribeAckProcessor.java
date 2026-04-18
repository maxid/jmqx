package plus.jmqx.broker.mqtt.message.impl;

import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttUnsubAckMessage;
import plus.jmqx.broker.mqtt.channel.MqttSession;
import plus.jmqx.broker.mqtt.message.MessageWrapper;
import plus.jmqx.broker.mqtt.message.NamespceMessageProcessor;
import reactor.util.context.ContextView;

import java.util.ArrayList;
import java.util.List;

/**
 * UNSUBACK 消息流程处理
 *
 * @author maxid
 * @since 2025/4/9 16:33
 */
public class UnsubscribeAckProcessor extends NamespceMessageProcessor<MqttUnsubAckMessage> {

    private static final List<MqttMessageType> MESSAGE_TYPES = new ArrayList<>();

    static {
        MESSAGE_TYPES.add(MqttMessageType.UNSUBACK);
    }

    /**
     * 返回处理的消息类型列表
     *
     * @return 消息类型列表
     */
    @Override
    public List<MqttMessageType> getMqttMessageTypes() {
        return MESSAGE_TYPES;
    }

    /**
     * 返回去订阅确认消息类型包装
     *
     * @return 去订阅确认消息类型包装类
     */
    @Override
    public Class<UnsubscribeAckMessageType> getMessageType() {
        return UnsubscribeAckMessageType.class;
    }

    /**
     * 处理去订阅确认消息并取消重试
     *
     * @param wrapper 消息包装
     * @param session 会话
     * @param view    上下文视图
     */
    @Override
    public void process(MessageWrapper<MqttUnsubAckMessage> wrapper, MqttSession session, ContextView view) {
        session.cancelRetry(MqttMessageType.UNSUBSCRIBE, wrapper.getMessage().variableHeader().messageId());
    }

}
