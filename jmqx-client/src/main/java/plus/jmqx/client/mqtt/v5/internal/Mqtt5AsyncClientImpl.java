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
 * <p>内部委托给 {@link Mqtt5RxClient} 的 Reactor API，将 Mono/Flux 转换为 CompletableFuture。
 *
 * @author maxid
 */
public class Mqtt5AsyncClientImpl implements Mqtt5AsyncClient {

    /**
     * 被委托的 Reactor API 客户端
     */
    private final Mqtt5RxClient rx;

    /**
     * 构造异步客户端实现。
     *
     * @param rx Reactor API 客户端
     */
    public Mqtt5AsyncClientImpl(Mqtt5RxClient rx) {
        this.rx = rx;
    }

    /**
     * 连接 MQTT broker。
     *
     * @return 携带 CONNACK 的 Future
     */
    @Override
    public CompletableFuture<Mqtt5ConnAck> connect() {
        return rx.connect().toFuture();
    }

    /**
     * 订阅并为每条匹配的 publish 调用回调。
     *
     * @param subscribe 订阅消息
     * @param callback  入站 publish 回调
     * @return 携带 SUBACK 的 Future
     */
    @Override
    public CompletableFuture<Mqtt5SubAck> subscribe(Mqtt5Subscribe subscribe, Consumer<Mqtt5Publish> callback) {
        rx.subscribePublishes(subscribe).doOnNext(callback).subscribe();
        return rx.subscribe(subscribe).toFuture();
    }

    /**
     * 发布一条 PUBLISH 消息。
     *
     * @param publish 待发布的消息
     * @return 携带发布结果的 Future
     */
    @Override
    public CompletableFuture<Mqtt5PublishResult> publish(Mqtt5Publish publish) {
        return rx.publish(publish).toFuture();
    }

    /**
     * 向 broker 发送 UNSUBSCRIBE。
     *
     * @param unsubscribe 取消订阅消息
     * @return 完成 Future
     */
    @Override
    public CompletableFuture<Void> unsubscribe(Mqtt5Unsubscribe unsubscribe) {
        return rx.unsubscribe(unsubscribe).toFuture();
    }

    /**
     * 发送 DISCONNECT 并关闭传输连接。
     *
     * @return 断开连接完成 Future
     */
    @Override
    public CompletableFuture<Void> disconnect() {
        return rx.disconnect().toFuture();
    }

    /**
     * 获取 MQTT 5.0 客户端配置。
     *
     * @return 客户端配置
     */
    @Override
    public Mqtt5ClientConfig getConfig() {
        return rx.getConfig();
    }

    /**
     * 获取客户端当前状态。
     *
     * @return 客户端状态
     */
    @Override
    public MqttClientState getState() {
        return rx.getState();
    }

    /**
     * 获取 MQTT 协议版本。
     *
     * @return MQTT 5.0
     */
    @Override
    public MqttVersion getVersion() {
        return rx.getVersion();
    }

    /**
     * 转换为 Reactor API。
     *
     * @return Reactor API 客户端
     */
    @Override
    public Mqtt5RxClient toRx() {
        return rx;
    }

    /**
     * 转换为阻塞 API。
     *
     * @return 阻塞客户端实现
     */
    @Override
    public Mqtt5BlockingClient toBlock() {
        return rx.toBlock();
    }

}
