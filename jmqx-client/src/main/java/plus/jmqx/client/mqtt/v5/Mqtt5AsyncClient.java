package plus.jmqx.client.mqtt.v5;

import plus.jmqx.client.mqtt.v5.message.Mqtt5ConnAck;
import plus.jmqx.client.mqtt.v5.message.Mqtt5Publish;
import plus.jmqx.client.mqtt.v5.message.Mqtt5SubAck;
import plus.jmqx.client.mqtt.v5.message.Mqtt5Subscribe;
import plus.jmqx.client.mqtt.v5.message.Mqtt5Unsubscribe;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * MQTT 5 客户端的异步 API（CompletableFuture + 回调）。
 *
 * @author maxid
 */
public interface Mqtt5AsyncClient extends Mqtt5Client {

    CompletableFuture<Mqtt5ConnAck> connect();

    CompletableFuture<Mqtt5SubAck> subscribe(Mqtt5Subscribe subscribe, Consumer<Mqtt5Publish> callback);

    CompletableFuture<Mqtt5PublishResult> publish(Mqtt5Publish publish);

    CompletableFuture<Void> unsubscribe(Mqtt5Unsubscribe unsubscribe);

    CompletableFuture<Void> disconnect();

    @Override
    default Mqtt5AsyncClient toAsync() {
        return this;
    }
}
