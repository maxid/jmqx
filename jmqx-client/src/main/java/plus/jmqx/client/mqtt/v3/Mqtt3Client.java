package plus.jmqx.client.mqtt.v3;

import plus.jmqx.client.mqtt.MqttClient;
import plus.jmqx.client.mqtt.MqttClientBuilder;

/**
 * MQTT 3.1.1 客户端（spec）。
 *
 * <p>通过 {@link MqttClientBuilder#useMqttVersion3()} 构建。
 *
 * @author maxid
 */
public interface Mqtt3Client extends MqttClient {

    @Override
    Mqtt3ClientConfig getConfig();

    /**
     * 转为 reactive API（Mono/Flux）。
     */
    Mqtt3RxClient toRx();

    /**
     * 转为异步 API（CompletableFuture + 回调）。
     */
    Mqtt3AsyncClient toAsync();

    /**
     * 转为阻塞 API。
     */
    Mqtt3BlockingClient toBlock();

    /**
     * 创建 MQTT 3 客户端 builder。
     */
    static Mqtt3ClientBuilder builder() {
        return new Mqtt3ClientBuilder();
    }
}
