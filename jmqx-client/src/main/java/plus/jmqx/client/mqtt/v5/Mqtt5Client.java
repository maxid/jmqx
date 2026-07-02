package plus.jmqx.client.mqtt.v5;

import plus.jmqx.client.mqtt.MqttClient;

/**
 * MQTT 5.0 客户端规范接口。
 *
 * <p>通过 {@link plus.jmqx.client.mqtt.MqttClientBuilder#useMqttVersion5()} 构建。
 *
 * @author maxid
 * @since 1.4.14
 */
public interface Mqtt5Client extends MqttClient {

    @Override
    Mqtt5ClientConfig getConfig();

    /**
     * @return Reactor API（{@link reactor.core.publisher.Mono}/{@link reactor.core.publisher.Flux}）
     */
    Mqtt5RxClient toRx();

    /**
     * @return CompletableFuture 异步 API
     */
    Mqtt5AsyncClient toAsync();

    /**
     * @return 阻塞 API
     */
    Mqtt5BlockingClient toBlock();

    /**
     * @return MQTT 5 客户端 builder
     */
    static Mqtt5ClientBuilder builder() {
        return new Mqtt5ClientBuilder();
    }

}
