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
 */
public interface Mqtt5BlockingClient extends Mqtt5Client {

    Mqtt5ConnAck connect();

    Mqtt5SubAck subscribe(Mqtt5Subscribe subscribe);

    Mqtt5Publishes publishes(MqttGlobalPublishFilter filter);

    void publish(Mqtt5Publish publish);

    void unsubscribe(Mqtt5Unsubscribe unsubscribe);

    void disconnect();

    @Override
    default Mqtt5BlockingClient toBlock() {
        return this;
    }
}
