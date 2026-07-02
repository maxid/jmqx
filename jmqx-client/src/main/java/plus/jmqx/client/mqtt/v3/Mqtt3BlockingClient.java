package plus.jmqx.client.mqtt.v3;

import plus.jmqx.client.mqtt.MqttGlobalPublishFilter;
import plus.jmqx.client.mqtt.v3.message.Mqtt3ConnAck;
import plus.jmqx.client.mqtt.v3.message.Mqtt3Publish;
import plus.jmqx.client.mqtt.v3.message.Mqtt3SubAck;
import plus.jmqx.client.mqtt.v3.message.Mqtt3Subscribe;
import plus.jmqx.client.mqtt.v3.message.Mqtt3Unsubscribe;

/**
 * MQTT 3 客户端的阻塞 API。
 *
 * @author maxid
 * @since 1.4.14
 */
public interface Mqtt3BlockingClient extends Mqtt3Client {

    /**
     * @return CONNACK
     */
    Mqtt3ConnAck connect();

    /**
     * @param subscribe 订阅消息
     * @return SUBACK
     */
    Mqtt3SubAck subscribe(Mqtt3Subscribe subscribe);

    /**
     * @param filter 入站消息过滤器
     * @return 可阻塞接收的 publish 句柄
     */
    Mqtt3Publishes publishes(MqttGlobalPublishFilter filter);

    /**
     * @param publish 待发布的消息
     */
    void publish(Mqtt3Publish publish);

    /**
     * @param unsubscribe 取消订阅消息
     */
    void unsubscribe(Mqtt3Unsubscribe unsubscribe);

    /**
     * 断开连接。
     */
    void disconnect();

    @Override
    default Mqtt3BlockingClient toBlock() {
        return this;
    }

}
