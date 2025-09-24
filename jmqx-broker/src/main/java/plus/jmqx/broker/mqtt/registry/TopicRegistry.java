package plus.jmqx.broker.mqtt.registry;

import io.netty.handler.codec.mqtt.MqttQoS;
import plus.jmqx.broker.mqtt.channel.MqttSession;
import plus.jmqx.broker.mqtt.topic.SubscribeTopic;
import plus.jmqx.broker.spi.DynamicLoader;

import java.util.Map;
import java.util.Set;

/**
 * Topic 注册中心
 *
 * @author maxid
 * @since 2025/4/9 11:58
 */
public interface TopicRegistry {

    TopicRegistry INSTANCE = DynamicLoader.findFirst(TopicRegistry.class).orElse(null);

    /**
     * 绑定主题跟会话关系
     *
     * @param topicFilter 订阅主题
     * @param session {@link MqttSession} 会话
     * @param qos         {@link MqttQoS} MQTT 质量服务等级
     */
    void registrySubscribeTopic(String topicFilter, MqttSession session, MqttQoS qos);

    /**
     * 绑定主题跟会话关系
     *
     * @param subscribeTopic {@link SubscribeTopic}
     */
    void registrySubscribeTopic(SubscribeTopic subscribeTopic);

    /**
     * 清除订阅消息
     *
     * @param mqttChannel {@link MqttSession}
     */
    void clear(MqttSession mqttChannel);

    /**
     * registryTopicConnection
     * 取消订阅关系
     *
     * @param subscribeTopic {@link SubscribeTopic}
     */
    void removeSubscribeTopic(SubscribeTopic subscribeTopic);

    /**
     * 获取topic的会话
     *
     * @param topicName topic name
     * @param qos       {@link MqttQoS}
     * @return {@link SubscribeTopic}
     */
    Set<SubscribeTopic> getSubscribesByTopic(String topicName, MqttQoS qos);

    /**
     * 绑定订阅关系
     *
     * @param subscribeTopics {@link SubscribeTopic}
     */
    void registrySubscribesTopic(Set<SubscribeTopic> subscribeTopics);

    /**
     * 获取所有topic信息
     *
     * @return {@link MqttSession}
     */
    Map<String, Set<MqttSession>> getAllTopics();

    /**
     * 获取总数
     *
     * @return counts
     */
    Integer counts();
}
