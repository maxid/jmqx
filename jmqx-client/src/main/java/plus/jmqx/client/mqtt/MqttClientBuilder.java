package plus.jmqx.client.mqtt;

import plus.jmqx.client.mqtt.v3.Mqtt3ClientBuilder;

/**
 * 顶层 builder —— 在此处选择版本。
 *
 * @author maxid
 */
public class MqttClientBuilder {

    /**
     * 使用 MQTT 3.1.1。
     */
    public Mqtt3ClientBuilder useMqttVersion3() {
        return new Mqtt3ClientBuilder();
    }

    /**
     * 使用 MQTT 5.0。
     */
    public plus.jmqx.client.mqtt.v5.Mqtt5ClientBuilder useMqttVersion5() {
        return new plus.jmqx.client.mqtt.v5.Mqtt5ClientBuilder();
    }
}
