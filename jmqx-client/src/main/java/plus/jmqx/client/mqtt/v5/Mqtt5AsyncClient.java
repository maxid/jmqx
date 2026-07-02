package plus.jmqx.client.mqtt.v5;

import plus.jmqx.client.mqtt.v5.message.Mqtt5ConnAck;
import plus.jmqx.client.mqtt.v5.message.Mqtt5Publish;
import plus.jmqx.client.mqtt.v5.message.Mqtt5SubAck;
import plus.jmqx.client.mqtt.v5.message.Mqtt5Subscribe;
import plus.jmqx.client.mqtt.v5.message.Mqtt5Unsubscribe;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * MQTT 5 客户端的异步 API（{@link CompletableFuture} + 回调）。
 *
 * @author maxid
 * @since 1.4.14
 */
public interface Mqtt5AsyncClient extends Mqtt5Client {

    /**
     * @return 连接完成后携带 CONNACK 的 Future
     */
    CompletableFuture<Mqtt5ConnAck> connect();

    /**
     * 订阅并为每条匹配的 publish 调用回调。
     *
     * @param subscribe 订阅消息
     * @param callback  入站 publish 回调
     * @return 携带 SUBACK 的 Future
     */
    CompletableFuture<Mqtt5SubAck> subscribe(Mqtt5Subscribe subscribe, Consumer<Mqtt5Publish> callback);

    /**
     * @param publish 待发布的消息
     * @return 携带发布结果的 Future
     */
    CompletableFuture<Mqtt5PublishResult> publish(Mqtt5Publish publish);

    /**
     * @param unsubscribe 取消订阅消息
     * @return 完成 Future
     */
    CompletableFuture<Void> unsubscribe(Mqtt5Unsubscribe unsubscribe);

    /**
     * @return 断开连接完成 Future
     */
    CompletableFuture<Void> disconnect();

    @Override
    default Mqtt5AsyncClient toAsync() {
        return this;
    }

}
