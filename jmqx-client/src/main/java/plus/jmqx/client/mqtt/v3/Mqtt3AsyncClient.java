package plus.jmqx.client.mqtt.v3;

import plus.jmqx.client.mqtt.v3.message.Mqtt3ConnAck;
import plus.jmqx.client.mqtt.v3.message.Mqtt3Publish;
import plus.jmqx.client.mqtt.v3.message.Mqtt3SubAck;
import plus.jmqx.client.mqtt.v3.message.Mqtt3Subscribe;
import plus.jmqx.client.mqtt.v3.message.Mqtt3Unsubscribe;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * MQTT 3 客户端的异步 API（CompletableFuture + 回调）。
 *
 * @author maxid
 * @since 1.4.14
 */
public interface Mqtt3AsyncClient extends Mqtt3Client {

    /**
     * 连接 broker。
     *
     * @return 连接完成后携带 CONNACK 的 Future
     */
    CompletableFuture<Mqtt3ConnAck> connect();

    /**
     * 订阅主题，并为每条匹配的入站 publish 调用回调。
     *
     * @param subscribe 订阅消息
     * @param callback  入站 publish 回调
     * @return 携带 SUBACK 的 Future
     */
    CompletableFuture<Mqtt3SubAck> subscribe(Mqtt3Subscribe subscribe, Consumer<Mqtt3Publish> callback);

    /**
     * 发布一条 PUBLISH 报文。
     *
     * @param publish 待发布的消息
     * @return 携带发布结果的 Future
     */
    CompletableFuture<Mqtt3PublishResult> publish(Mqtt3Publish publish);

    /**
     * 取消订阅。
     *
     * @param unsubscribe 取消订阅消息
     * @return 完成 Future
     */
    CompletableFuture<Void> unsubscribe(Mqtt3Unsubscribe unsubscribe);

    /**
     * 断开连接。
     *
     * @return 断开连接完成 Future
     */
    CompletableFuture<Void> disconnect();

    @Override
    default Mqtt3AsyncClient toAsync() {
        return this;
    }

}
