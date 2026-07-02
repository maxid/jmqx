package plus.jmqx.client.mqtt.v3;

import plus.jmqx.client.mqtt.MqttGlobalPublishFilter;
import plus.jmqx.client.mqtt.v3.message.Mqtt3ConnAck;
import plus.jmqx.client.mqtt.v3.message.Mqtt3Publish;
import plus.jmqx.client.mqtt.v3.message.Mqtt3Subscribe;
import plus.jmqx.client.mqtt.v3.message.Mqtt3SubAck;
import plus.jmqx.client.mqtt.v3.message.Mqtt3Unsubscribe;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * MQTT 3 客户端的 reactive API（Reactor Mono/Flux）。
 *
 * <p>所有方法返回 cold publisher —— 订阅时才执行。
 *
 * @author maxid
 */
public interface Mqtt3RxClient extends Mqtt3Client {

    /**
     * 以默认 CONNECT 连接。订阅返回的 Mono 时才连接。
     */
    Mono<Mqtt3ConnAck> connect();

    /**
     * 以给定 SUBSCRIBE 订阅。订阅返回的 Mono 时才订阅。
     *
     * @return SUBACK；若所有订阅被拒绝则 error。
     */
    Mono<Mqtt3SubAck> subscribe(Mqtt3Subscribe subscribe);

    /**
     * 订阅并消费匹配的 PUBLISH。返回的 Flux 同时发射匹配的 publish（先完成 SUBACK）。
     *
     * @param subscribe 订阅消息。
     * @return 匹配的 publish 流。
     */
    Flux<Mqtt3Publish> subscribePublishes(Mqtt3Subscribe subscribe);

    /**
     * 全局消费所有匹配给定过滤器的入站 PUBLISH。
     */
    Flux<Mqtt3Publish> publishes(MqttGlobalPublishFilter filter);

    /**
     * 发布一条 PUBLISH。QoS1/2 在收到 ACK 时完成；QoS0 在发送时完成。
     *
     * @return 发布结果（含可能的错误）。
     */
    Mono<Mqtt3PublishResult> publish(Mqtt3Publish publish);

    /**
     * 以给定 UNSUBSCRIBE 取消订阅。
     */
    Mono<Void> unsubscribe(Mqtt3Unsubscribe unsubscribe);

    /**
     * 断开连接。
     */
    Mono<Void> disconnect();

    @Override
    default Mqtt3RxClient toRx() {
        return this;
    }
}
