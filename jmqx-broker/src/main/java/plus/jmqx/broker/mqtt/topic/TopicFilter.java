package plus.jmqx.broker.mqtt.topic;

import io.netty.handler.codec.mqtt.MqttQoS;
import plus.jmqx.broker.mqtt.channel.MqttSession;

import java.util.Set;

/**
 * MQTT 主题过滤器
 *
 * @author maxid
 * @since 2025/4/16 09:27
 */
public interface TopicFilter {

    /**
     * 获取订阅主题集合。
     *
     * @param topic   主题
     * @param mqttQoS QoS
     * @return 订阅集合
     */
    Set<SubscribeTopic> getSubscribeByTopic(String topic, MqttQoS mqttQoS);


    /**
     * 保存订阅主题。
     *
     * @param topicFilter 主题过滤器
     * @param mqttQoS     QoS
     * @param mqttChannel 会话
     */
    void addSubscribeTopic(String topicFilter, MqttSession mqttChannel, MqttQoS mqttQoS);

    /**
     * 保存订阅对象。
     *
     * @param subscribeTopic 订阅对象
     */
    void addSubscribeTopic(SubscribeTopic subscribeTopic);

    /**
     * 删除订阅对象。
     *
     * @param subscribeTopic 订阅对象
     */
    void removeSubscribeTopic(SubscribeTopic subscribeTopic);

    /**
     * 获取订阅总数。
     *
     * @return 订阅数量
     */
    int count();

    /**
     * 获取所有订阅对象。
     *
     * @return 订阅集合
     */
    Set<SubscribeTopic> getAllSubscribesTopic();

}
