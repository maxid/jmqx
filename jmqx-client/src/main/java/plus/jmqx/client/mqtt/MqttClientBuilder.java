package plus.jmqx.client.mqtt;

/**
 * 顶层 builder —— 在此处选择 MQTT 协议版本。
 *
 * @author maxid
 * @since 1.4.14
 */
public class MqttClientBuilder {

    /**
     * 使用 MQTT 3.1.1 协议构建客户端。
     *
     * @return MQTT 3 客户端 builder
     */
    public plus.jmqx.client.mqtt.v3.Mqtt3ClientBuilder useMqttVersion3() {
        return new plus.jmqx.client.mqtt.v3.Mqtt3ClientBuilder();
    }

    /**
     * 使用 MQTT 5.0 协议构建客户端。
     *
     * @return MQTT 5 客户端 builder
     */
    public plus.jmqx.client.mqtt.v5.Mqtt5ClientBuilder useMqttVersion5() {
        return new plus.jmqx.client.mqtt.v5.Mqtt5ClientBuilder();
    }

}
