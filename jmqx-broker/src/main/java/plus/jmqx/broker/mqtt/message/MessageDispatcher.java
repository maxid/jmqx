package plus.jmqx.broker.mqtt.message;

import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import plus.jmqx.broker.config.Configuration;
import plus.jmqx.broker.mqtt.channel.MqttSession;
import plus.jmqx.broker.mqtt.context.ReceiveContext;
import plus.jmqx.broker.mqtt.message.dispatch.PublishMessage;
import plus.jmqx.broker.mqtt.message.interceptor.Intercept;
import plus.jmqx.broker.mqtt.message.interceptor.MessageProxy;
import plus.jmqx.broker.spi.DynamicLoader;

/**
 * 消息报文分发处理器
 *
 * @author maxid
 * @since 2025/4/9 14:18
 */
public interface MessageDispatcher {
    /**
     * 用户自定义实例
     */
    MessageDispatcher INSTANCE = DynamicLoader.findFirst(MessageDispatcher.class).orElse(null);

    /**
     * 消息处理代理工具
     */
    MessageProxy MESSAGE_PROXY = new MessageProxy();

    /**
     * 根据消息类型分发消息至相应消息处理器进行消息处理
     *
     * @param session {@link MqttSession} 连接会话
     * @param wrapper {@link MessageWrapper} 消息
     * @param context {@link ReceiveContext} 上下文
     * @param <C>     配置类型
     */
    @Intercept
    <C extends Configuration> void dispatch(MqttSession session, MessageWrapper<MqttMessage> wrapper, ReceiveContext<C> context);

    /**
     * 下发消息
     *
     * @param message 消息
     */
    void publish(MqttPublishMessage message);

    /**
     * 消息处理适配器代理
     *
     * @return 消息处理适配器代理
     */
    default MessageDispatcher proxy() {
        return MESSAGE_PROXY.proxy(this);
    }
}
