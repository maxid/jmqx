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
 */
public interface Mqtt3BlockingClient extends Mqtt3Client {

    Mqtt3ConnAck connect();

    Mqtt3SubAck subscribe(Mqtt3Subscribe subscribe);

    /**
     * 全局消费入站 publish。返回可阻塞接收的 {@link Mqtt3Publishes}。
     */
    Mqtt3Publishes publishes(MqttGlobalPublishFilter filter);

    void publish(Mqtt3Publish publish);

    void unsubscribe(Mqtt3Unsubscribe unsubscribe);

    void disconnect();

    @Override
    default Mqtt3BlockingClient toBlock() {
        return this;
    }
}
