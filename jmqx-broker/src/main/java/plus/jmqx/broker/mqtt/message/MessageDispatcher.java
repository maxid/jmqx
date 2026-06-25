package plus.jmqx.broker.mqtt.message;

import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import plus.jmqx.broker.config.Configuration;
import plus.jmqx.broker.mqtt.channel.MqttSession;
import plus.jmqx.broker.mqtt.context.ReceiveContext;
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
     * @param session 连接会话
     * @param wrapper 消息包装
     * @param context 上下文
     * @param <C>     配置类型
     */
    @Intercept
    <C extends Configuration> void dispatch(MqttSession session, MessageWrapper<MqttMessage> wrapper, ReceiveContext<C> context);

    /**
     * 下发消息（基于主题路由，投递到所有匹配的订阅者）
     *
     * @param message 发布消息
     */
    void publish(MqttPublishMessage message);

    /**
     * 向指定客户端设备下发消息（直接写入设备 Session，不经过主题路由）
     * <p>
     * 通过 {@link MessageWrapper#clientId} 标识定向投递，
     * 走完整分发管线（含 ACL 检查），集群模式由 TailIntercept 自动扩散。
     *
     * @param clientId 目标设备 clientId
     * @param message  发布消息
     */
    void publish(String clientId, MqttPublishMessage message);

    /**
     * 消息处理适配器代理
     *
     * @return 代理后的分发器
     */
    default MessageDispatcher proxy() {
        return MESSAGE_PROXY.proxy(this);
    }

}
