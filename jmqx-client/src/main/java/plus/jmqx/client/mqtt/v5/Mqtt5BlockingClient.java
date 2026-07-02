package plus.jmqx.client.mqtt.v5;

import plus.jmqx.client.mqtt.MqttGlobalPublishFilter;
import plus.jmqx.client.mqtt.v5.message.Mqtt5ConnAck;
import plus.jmqx.client.mqtt.v5.message.Mqtt5Publish;
import plus.jmqx.client.mqtt.v5.message.Mqtt5SubAck;
import plus.jmqx.client.mqtt.v5.message.Mqtt5Subscribe;
import plus.jmqx.client.mqtt.v5.message.Mqtt5Unsubscribe;

/**
 * MQTT 5 客户端的阻塞 API。
 *
 * @author maxid
 * @since 1.4.14
 */
public interface Mqtt5BlockingClient extends Mqtt5Client {

    /**
     * @return CONNACK
     */
    Mqtt5ConnAck connect();

    /**
     * @param subscribe 订阅消息
     * @return SUBACK
     */
    Mqtt5SubAck subscribe(Mqtt5Subscribe subscribe);

    /**
     * @param filter 入站消息过滤器
     * @return 可阻塞接收的 publish 句柄
     */
    Mqtt5Publishes publishes(MqttGlobalPublishFilter filter);

    /**
     * @param publish 待发布的消息
     */
    void publish(Mqtt5Publish publish);

    /**
     * @param unsubscribe 取消订阅消息
     */
    void unsubscribe(Mqtt5Unsubscribe unsubscribe);

    /**
     * 断开连接。
     */
    void disconnect();

    @Override
    default Mqtt5BlockingClient toBlock() {
        return this;
    }

}
