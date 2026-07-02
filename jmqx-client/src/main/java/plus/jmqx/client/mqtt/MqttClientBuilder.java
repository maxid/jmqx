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
     * 使用 MQTT 5.0（Task 21+ 实现）。
     */
    public Object useMqttVersion5() {
        throw new UnsupportedOperationException("MQTT 5 added in Task 21+");
    }
}
