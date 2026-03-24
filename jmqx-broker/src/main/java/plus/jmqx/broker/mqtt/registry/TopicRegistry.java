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
     * @param session     会话
     * @param qos         MQTT 质量服务等级
     */
    void registrySubscribeTopic(String topicFilter, MqttSession session, MqttQoS qos);

    /**
     * 绑定主题跟会话关系
     *
     * @param subscribeTopic 订阅对象
     */
    void registrySubscribeTopic(SubscribeTopic subscribeTopic);

    /**
     * 清除订阅消息
     *
     * @param mqttChannel 会话
     */
    void clear(MqttSession mqttChannel);

    /**
     * registryTopicConnection
     * 取消订阅关系
     *
     * @param subscribeTopic 订阅对象
     */
    void removeSubscribeTopic(SubscribeTopic subscribeTopic);

    /**
     * 获取topic的会话
     *
     * @param topicName 主题名称
     * @param qos       MQTT 质量服务等级
     * @return 订阅集合
     */
    Set<SubscribeTopic> getSubscribesByTopic(String topicName, MqttQoS qos);

    /**
     * 绑定订阅关系
     *
     * @param subscribeTopics 订阅集合
     */
    void registrySubscribesTopic(Set<SubscribeTopic> subscribeTopics);

    /**
     * 获取所有topic信息
     *
     * @return 主题与会话映射
     */
    Map<String, Set<MqttSession>> getAllTopics();

    /**
     * 获取总数
     *
     * @return 订阅总数
     */
    Integer counts();

}
