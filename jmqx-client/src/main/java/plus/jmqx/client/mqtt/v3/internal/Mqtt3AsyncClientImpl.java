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
 * {@link Mqtt3AsyncClient} 的 CompletableFuture 实现。
 *
 * <p>所有操作委托给 {@link Mqtt3RxClient}，通过 Reactor 的 {@code Mono.toFuture()} 转换。
 *
 * @author maxid
 */
public class Mqtt3AsyncClientImpl implements Mqtt3AsyncClient {

    /** 被委托的 Reactive API 客户端 */
    private final Mqtt3RxClient rx;

    /**
     * 构造异步客户端实现。
     *
     * @param rx 被委托的 Reactive 客户端
     */
    public Mqtt3AsyncClientImpl(Mqtt3RxClient rx) {
        this.rx = rx;
    }

    @Override
    public CompletableFuture<Mqtt3ConnAck> connect() {
        return rx.connect().toFuture();
    }

    @Override
    public CompletableFuture<Mqtt3SubAck> subscribe(Mqtt3Subscribe subscribe, Consumer<Mqtt3Publish> callback) {
        // 先订阅 publish 流（注册回调），再发送 SUBSCRIBE 报文；两者共享同一底层订阅。
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
    public Mqtt3RxClient toRx() {
        return rx;
    }

    @Override
    public Mqtt3BlockingClient toBlock() {
        return rx.toBlock();
    }

}
