package plus.jmqx.client.mqtt.v5.internal;

import plus.jmqx.client.mqtt.MqttClientState;
import plus.jmqx.client.mqtt.MqttVersion;
import plus.jmqx.client.mqtt.v5.Mqtt5AsyncClient;
import plus.jmqx.client.mqtt.v5.Mqtt5BlockingClient;
import plus.jmqx.client.mqtt.v5.Mqtt5ClientConfig;
import plus.jmqx.client.mqtt.v5.Mqtt5PublishResult;
import plus.jmqx.client.mqtt.v5.Mqtt5RxClient;
import plus.jmqx.client.mqtt.v5.message.Mqtt5ConnAck;
import plus.jmqx.client.mqtt.v5.message.Mqtt5Publish;
import plus.jmqx.client.mqtt.v5.message.Mqtt5SubAck;
import plus.jmqx.client.mqtt.v5.message.Mqtt5Subscribe;
import plus.jmqx.client.mqtt.v5.message.Mqtt5Unsubscribe;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * {@link Mqtt5AsyncClient} 的 CompletableFuture 实现。
 *
 * @author maxid
 */
public class Mqtt5AsyncClientImpl implements Mqtt5AsyncClient {

    private final Mqtt5RxClient rx;

    public Mqtt5AsyncClientImpl(Mqtt5RxClient rx) {
        this.rx = rx;
    }

    @Override
    public CompletableFuture<Mqtt5ConnAck> connect() {
        return rx.connect().toFuture();
    }

    @Override
    public CompletableFuture<Mqtt5SubAck> subscribe(Mqtt5Subscribe subscribe, Consumer<Mqtt5Publish> callback) {
        rx.subscribePublishes(subscribe).doOnNext(callback).subscribe();
        return rx.subscribe(subscribe).toFuture();
    }

    @Override
    public CompletableFuture<Mqtt5PublishResult> publish(Mqtt5Publish publish) {
        return rx.publish(publish).toFuture();
    }

    @Override
    public CompletableFuture<Void> unsubscribe(Mqtt5Unsubscribe unsubscribe) {
        return rx.unsubscribe(unsubscribe).toFuture();
    }

    @Override
    public CompletableFuture<Void> disconnect() {
        return rx.disconnect().toFuture();
    }

    @Override
    public Mqtt5ClientConfig getConfig() {
        return rx.getConfig();
    }

    @Override
    public MqttClientState getState() {
        return rx.getState();
    }

    @Override
    public MqttVersion getVersion() {
        return rx.getVersion();
    }

    @Override
    public Mqtt5AsyncClient toAsync() {
        return this;
    }

    @Override
    public Mqtt5RxClient toRx() {
        return rx;
    }

    @Override
    public Mqtt5BlockingClient toBlock() {
        return rx.toBlock();
    }
}
