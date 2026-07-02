package plus.jmqx.client.mqtt.v5;

import plus.jmqx.client.mqtt.MqttClient;

/**
 * MQTT 5.0 客户端（spec）。
 *
 * @author maxid
 */
public interface Mqtt5Client extends MqttClient {

    @Override
    Mqtt5ClientConfig getConfig();

    Mqtt5RxClient toRx();

    Mqtt5AsyncClient toAsync();

    Mqtt5BlockingClient toBlock();

    static Mqtt5ClientBuilder builder() {
        return new Mqtt5ClientBuilder();
    }
}
