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
 */
public interface Mqtt3AsyncClient extends Mqtt3Client {

    CompletableFuture<Mqtt3ConnAck> connect();

    /**
     * 订阅，并为每条匹配的 publish 调用 callback。
     */
    CompletableFuture<Mqtt3SubAck> subscribe(Mqtt3Subscribe subscribe, Consumer<Mqtt3Publish> callback);

    CompletableFuture<Mqtt3PublishResult> publish(Mqtt3Publish publish);

    CompletableFuture<Void> unsubscribe(Mqtt3Unsubscribe unsubscribe);

    CompletableFuture<Void> disconnect();

    @Override
    default Mqtt3AsyncClient toAsync() {
        return this;
    }
}
