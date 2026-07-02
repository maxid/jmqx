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
     * @return Reactor API（Mono/Flux）
     */
    Mqtt3RxClient toRx();

    /**
     * @return CompletableFuture 异步 API
     */
    Mqtt3AsyncClient toAsync();

    /**
     * @return 阻塞 API
     */
    Mqtt3BlockingClient toBlock();

    /**
     * @return MQTT 3 客户端 builder
     */
    static Mqtt3ClientBuilder builder() {
        return new Mqtt3ClientBuilder();
    }

}
