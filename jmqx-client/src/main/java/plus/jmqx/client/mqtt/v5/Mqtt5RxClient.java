package plus.jmqx.client.mqtt.v5;

import plus.jmqx.client.mqtt.MqttGlobalPublishFilter;
import plus.jmqx.client.mqtt.v5.message.Mqtt5ConnAck;
import plus.jmqx.client.mqtt.v5.message.Mqtt5Publish;
import plus.jmqx.client.mqtt.v5.message.Mqtt5SubAck;
import plus.jmqx.client.mqtt.v5.message.Mqtt5Subscribe;
import plus.jmqx.client.mqtt.v5.message.Mqtt5Unsubscribe;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * MQTT 5 客户端的 reactive API（Reactor {@link Mono}/{@link Flux}）。
 *
 * <p>所有方法返回 cold publisher —— 订阅时才执行网络 I/O。
 *
 * @author maxid
 * @since 1.4.14
 */
public interface Mqtt5RxClient extends Mqtt5Client {

    /**
     * 以默认 CONNECT 连接 broker。订阅返回的 {@link Mono} 时才发起连接。
     *
     * @return CONNACK；连接被拒绝时以 error 结束
     */
    Mono<Mqtt5ConnAck> connect();

    /**
     * 向 broker 发送 SUBSCRIBE。
     *
     * @param subscribe 订阅消息
     * @return SUBACK；若所有订阅被拒绝则以 error 结束
     */
    Mono<Mqtt5SubAck> subscribe(Mqtt5Subscribe subscribe);

    /**
     * 订阅并消费匹配主题过滤器的入站 PUBLISH。
     *
     * @param subscribe 订阅消息
     * @return 匹配的入站 publish 流
     */
    Flux<Mqtt5Publish> subscribePublishes(Mqtt5Subscribe subscribe);

    /**
     * 全局消费所有匹配给定过滤器的入站 PUBLISH。
     *
     * @param filter 入站消息过滤器
     * @return 入站 publish 流
     */
    Flux<Mqtt5Publish> publishes(MqttGlobalPublishFilter filter);

    /**
     * 发布一条 PUBLISH。QoS1/2 在收到 ACK 时完成；QoS0 在发送后完成。
     *
     * @param publish 待发布的消息
     * @return 发布结果（含可能的错误）
     */
    Mono<Mqtt5PublishResult> publish(Mqtt5Publish publish);

    /**
     * 向 broker 发送 UNSUBSCRIBE。
     *
     * @param unsubscribe 取消订阅消息
     * @return 完成信号
     */
    Mono<Void> unsubscribe(Mqtt5Unsubscribe unsubscribe);

    /**
     * 发送 DISCONNECT 并关闭传输连接。
     *
     * @return 完成信号
     */
    Mono<Void> disconnect();

    @Override
    default Mqtt5RxClient toRx() {
        return this;
    }

}
