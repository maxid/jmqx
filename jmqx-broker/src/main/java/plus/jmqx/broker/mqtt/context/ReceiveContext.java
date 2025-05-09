package plus.jmqx.broker.mqtt.context;

import io.netty.handler.codec.mqtt.MqttMessage;
import plus.jmqx.broker.acl.AclManager;
import plus.jmqx.broker.auth.AuthManager;
import plus.jmqx.broker.cluster.ClusterRegistry;
import plus.jmqx.broker.config.Configuration;
import plus.jmqx.broker.mqtt.message.dispatch.PlatformDispatcher;
import plus.jmqx.broker.mqtt.registry.SessionRegistry;
import plus.jmqx.broker.mqtt.channel.MqttSession;
import plus.jmqx.broker.mqtt.message.MessageDispatcher;
import plus.jmqx.broker.mqtt.registry.EventRegistry;
import plus.jmqx.broker.mqtt.registry.MessageRegistry;
import plus.jmqx.broker.mqtt.message.MessageWrapper;
import plus.jmqx.broker.mqtt.retry.TimeAckManager;
import plus.jmqx.broker.mqtt.registry.TopicRegistry;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * 上下文
 *
 * @author maxid
 * @since 2025/4/9 11:55
 */
public interface ReceiveContext<C extends Configuration> extends BiConsumer<MqttSession, MessageWrapper<MqttMessage>> {

    /**
     * 获取配置信息
     *
     * @return {@link Configuration} 配置信息
     */
    C getConfiguration();

    /**
     * 确认机制管理器
     *
     * @return 确认机制管理器
     */
    TimeAckManager getTimeAckManager();

    /**
     * 集群注册中心
     *
     * @return {@link ClusterRegistry} 集群注册中心
     */
    ClusterRegistry getClusterRegistry();

    /**
     * 事件注册中心
     *
     * @return {@link EventRegistry} 事件注册中心
     */
    EventRegistry getEventRegistry();

    /**
     * 会话管理中心
     *
     * @return {@link SessionRegistry} 会话管理中心
     */
    SessionRegistry getSessionRegistry();

    /**
     * MQTT 主题注册中心
     *
     * @return MQTT 主题注册中心
     */
    TopicRegistry getTopicRegistry();

    /**
     * MQTT 消息注册中心
     *
     * @return MQTT 消息注册中心
     */
    MessageRegistry getMessageRegistry();

    /**
     * 获取 MQTT 消息报文分发处理器
     *
     * @return 消息报文分发处理器
     */
    MessageDispatcher getMessageDispatcher();

    /**
     * MQTT 主题访问控制管理器
     *
     * @return MQTT 主题访问控制管理器
     */
    AclManager getAclManager();

    /**
     * MQTT 连接认证管理器
     *
     * @return MQTT 连接认证管理器
     */
    AuthManager getAuthManager();

    /**
     * MQTT 生命周期分发
     *
     * @param consumer 订阅
     */
    void dispatch(Consumer<PlatformDispatcher> consumer);
}
