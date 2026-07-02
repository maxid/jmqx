package plus.jmqx.client.mqtt.v3;

import plus.jmqx.client.mqtt.MqttClient;
import plus.jmqx.client.mqtt.MqttClientBuilder;

/**
 * MQTT 3.1.1 客户端规范接口。
 *
 * <p>通过 {@link MqttClientBuilder#useMqttVersion3()} 构建。
 *
 * @author maxid
 * @since 1.4.14
 */
public interface Mqtt3Client extends MqttClient {

    @Override
    Mqtt3ClientConfig getConfig();

    /**
     * 转换为 Reactor API（Mono/Flux）。
     *
     * @return Reactor API 客户端
     */
    Mqtt3RxClient toRx();

    /**
     * 转换为 CompletableFuture 异步 API。
     *
     * @return 异步 API 客户端
     */
    Mqtt3AsyncClient toAsync();

    /**
     * 转换为阻塞 API。
     *
     * @return 阻塞 API 客户端
     */
    Mqtt3BlockingClient toBlock();

    /**
     * 创建 MQTT 3 客户端 builder。
     *
     * @return MQTT 3 客户端 builder
     */
    static Mqtt3ClientBuilder builder() {
        return new Mqtt3ClientBuilder();
    }

}
