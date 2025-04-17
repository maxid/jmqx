package plus.jmqx.broker.mqtt.message;

import io.netty.handler.codec.mqtt.MqttMessage;
import plus.jmqx.broker.config.Configuration;
import plus.jmqx.broker.mqtt.channel.MqttChannel;
import plus.jmqx.broker.mqtt.context.ReceiveContext;
import plus.jmqx.broker.mqtt.message.interceptor.MessageProxy;
import plus.jmqx.broker.spi.DynamicLoader;

/**
 * 消息处理适配器
 *
 * @author maxid
 * @since 2025/4/9 14:18
 */
public interface MessageAdapter {
    /**
     * 用户自定义实例
     */
    MessageAdapter INSTANCE = DynamicLoader.findFirst(MessageAdapter.class).orElse(null);

    /**
     * 消息处理代理工具
     */
    MessageProxy MESSAGE_PROXY = new MessageProxy();

    /**
     * 根据消息类型分发消息至相应消息处理器进行消息处理
     *
     * @param session {@link MqttChannel} 连接会话
     * @param message {@link MessageWrapper} 消息
     * @param context {@link ReceiveContext} 上下文
     * @param <C>     配置类型
     */
    <C extends Configuration> void dispatch(MqttChannel session, MessageWrapper<MqttMessage> message, ReceiveContext<C> context);

    /**
     * 消息处理适配器代理
     *
     * @return 消息处理适配器代理
     */
    default MessageAdapter proxy() {
        return MESSAGE_PROXY.proxy(this);
    }
}
