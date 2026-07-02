package plus.jmqx.client.mqtt.v3.internal;

import plus.jmqx.client.mqtt.MqttClientState;
import plus.jmqx.client.mqtt.MqttVersion;
import plus.jmqx.client.mqtt.v3.Mqtt3AsyncClient;
import plus.jmqx.client.mqtt.v3.Mqtt3BlockingClient;
import plus.jmqx.client.mqtt.v3.Mqtt3ClientConfig;
import plus.jmqx.client.mqtt.v3.Mqtt3PublishResult;
import plus.jmqx.client.mqtt.v3.Mqtt3RxClient;
import plus.jmqx.client.mqtt.v3.message.Mqtt3ConnAck;
import plus.jmqx.client.mqtt.v3.message.Mqtt3Publish;
import plus.jmqx.client.mqtt.v3.message.Mqtt3SubAck;
import plus.jmqx.client.mqtt.v3.message.Mqtt3Subscribe;
import plus.jmqx.client.mqtt.v3.message.Mqtt3Unsubscribe;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * {@link Mqtt3AsyncClient} 的 CompletableFuture 实现，委托给 {@link Mqtt3RxClient}。
 *
 * <p>Reactor 的 {@code Mono.toFuture()} / {@code Flux} 订阅负责线程切换与背压传递。
 *
 * @author maxid
 */
public class Mqtt3AsyncClientImpl implements Mqtt3AsyncClient {

    private final Mqtt3RxClient rx;

    public Mqtt3AsyncClientImpl(Mqtt3RxClient rx) {
        this.rx = rx;
    }

    @Override
    public CompletableFuture<Mqtt3ConnAck> connect() {
        return rx.connect().toFuture();
    }

    @Override
    public CompletableFuture<Mqtt3SubAck> subscribe(Mqtt3Subscribe subscribe, Consumer<Mqtt3Publish> callback) {
        // 先订阅 publish 流（回调），再发送 SUBSCRIBE；两者共享同一订阅。
        rx.subscribePublishes(subscribe).doOnNext(callback).subscribe();
        return rx.subscribe(subscribe).toFuture();
    }

    @Override
    public CompletableFuture<Mqtt3PublishResult> publish(Mqtt3Publish publish) {
        return rx.publish(publish).toFuture();
    }

    @Override
    public CompletableFuture<Void> unsubscribe(Mqtt3Unsubscribe unsubscribe) {
        return rx.unsubscribe(unsubscribe).toFuture();
    }

    @Override
    public CompletableFuture<Void> disconnect() {
        return rx.disconnect().toFuture();
    }

    @Override
    public Mqtt3ClientConfig getConfig() {
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
    public Mqtt3AsyncClient toAsync() {
        return this;
    }

    @Override
    public Mqtt3RxClient toRx() {
        return rx;
    }

    @Override
    public Mqtt3BlockingClient toBlock() {
        return rx.toBlock();
    }
}
