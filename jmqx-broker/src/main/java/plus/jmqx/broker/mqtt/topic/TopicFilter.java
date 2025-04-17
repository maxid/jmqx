package plus.jmqx.broker.mqtt.topic;

import io.netty.handler.codec.mqtt.MqttQoS;
import plus.jmqx.broker.mqtt.channel.MqttChannel;

import java.util.Set;

/**
 * MQTT 主题过滤器
 *
 * @author maxid
 * @since 2025/4/16 09:27
 */
public interface TopicFilter {
    /**
     * 获取订阅 Topic
     *
     * @param topic   {@link String} 主题
     * @param mqttQoS {@link MqttQoS} QoS
     * @return {@link SubscribeTopic} 订阅主题集合
     */
    Set<SubscribeTopic> getSubscribeByTopic(String topic, MqttQoS mqttQoS);


    /**
     * 保存订阅 Topic
     *
     * @param topicFilter topicFilter
     * @param mqttQoS     {@link MqttQoS}
     * @param mqttChannel {@link MqttChannel}
     */
    void addSubscribeTopic(String topicFilter, MqttChannel mqttChannel, MqttQoS mqttQoS);

    /**
     * 保存订阅 Topic
     *
     * @param subscribeTopic {@link SubscribeTopic}
     */
    void addSubscribeTopic(SubscribeTopic subscribeTopic);

    /**
     * 删除订阅 Topic
     *
     * @param subscribeTopic {@link SubscribeTopic}
     */
    void removeSubscribeTopic(SubscribeTopic subscribeTopic);

    /**
     * 获取订阅总数
     *
     * @return 总数
     */
    int count();

    /**
     * 获取订所有订阅 Topic
     *
     * @return {@link SubscribeTopic}
     */
    Set<SubscribeTopic> getAllSubscribesTopic();
}
