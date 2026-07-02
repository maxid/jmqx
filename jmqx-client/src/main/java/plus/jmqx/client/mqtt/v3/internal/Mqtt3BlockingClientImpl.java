package plus.jmqx.client.mqtt.v3.internal;

import lombok.extern.slf4j.Slf4j;
import plus.jmqx.client.mqtt.MqttClientState;
import plus.jmqx.client.mqtt.MqttGlobalPublishFilter;
import plus.jmqx.client.mqtt.MqttVersion;
import plus.jmqx.client.mqtt.v3.Mqtt3AsyncClient;
import plus.jmqx.client.mqtt.v3.Mqtt3BlockingClient;
import plus.jmqx.client.mqtt.v3.Mqtt3ClientConfig;
import plus.jmqx.client.mqtt.v3.Mqtt3Publishes;
import plus.jmqx.client.mqtt.v3.Mqtt3RxClient;
import plus.jmqx.client.mqtt.v3.message.Mqtt3ConnAck;
import plus.jmqx.client.mqtt.v3.message.Mqtt3Publish;
import plus.jmqx.client.mqtt.v3.message.Mqtt3SubAck;
import plus.jmqx.client.mqtt.v3.message.Mqtt3Subscribe;
import plus.jmqx.client.mqtt.v3.message.Mqtt3Unsubscribe;
import reactor.core.Disposable;

import java.time.Duration;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * {@link Mqtt3BlockingClient} 的同步实现。
 *
 * <p>所有操作委托给 {@link Mqtt3RxClient}，以 30 秒超时阻塞等待结果。
 * {@link #publishes(MqttGlobalPublishFilter)} 返回基于 {@link LinkedBlockingQueue}
 * 的 {@link Mqtt3Publishes}，将入站流桥接到阻塞迭代。
 *
 * @author maxid
 */
@Slf4j
public class Mqtt3BlockingClientImpl implements Mqtt3BlockingClient {

    /**
     * 默认阻塞超时时间：30 秒
     */
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    /**
     * 被委托的 Reactive API 客户端
     */
    private final Mqtt3RxClient rx;

    /**
     * 构造阻塞客户端实现。
     *
     * @param rx 被委托的 Reactive 客户端
     */
    public Mqtt3BlockingClientImpl(Mqtt3RxClient rx) {
        this.rx = rx;
    }

    @Override
    public Mqtt3ConnAck connect() {
        return rx.connect().block(TIMEOUT);
    }

    @Override
    public Mqtt3SubAck subscribe(Mqtt3Subscribe subscribe) {
        return rx.subscribe(subscribe).block(TIMEOUT);
    }

    @Override
    public Mqtt3Publishes publishes(MqttGlobalPublishFilter filter) {
        LinkedBlockingQueue<Mqtt3Publish> queue = new LinkedBlockingQueue<>();
        Disposable sub = rx.publishes(filter).subscribe(queue::offer);
        return Mqtt3Publishes.fromQueue(queue, sub::dispose);
    }

    @Override
    public void publish(Mqtt3Publish publish) {
        var result = rx.publish(publish).block(TIMEOUT);
        if (result != null && result.getError() != null) {
            throw new RuntimeException("PUBLISH 失败", result.getError());
        }
    }

    @Override
    public void unsubscribe(Mqtt3Unsubscribe unsubscribe) {
        rx.unsubscribe(unsubscribe).block(TIMEOUT);
    }

    @Override
    public void disconnect() {
        rx.disconnect().block(TIMEOUT);
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
        return rx.toAsync();
    }

    @Override
    public Mqtt3RxClient toRx() {
        return rx;
    }

}
