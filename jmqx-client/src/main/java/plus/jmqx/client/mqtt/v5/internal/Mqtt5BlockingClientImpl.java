package plus.jmqx.client.mqtt.v5.internal;

import plus.jmqx.client.mqtt.MqttClientState;
import plus.jmqx.client.mqtt.MqttGlobalPublishFilter;
import plus.jmqx.client.mqtt.MqttVersion;
import plus.jmqx.client.mqtt.v5.Mqtt5AsyncClient;
import plus.jmqx.client.mqtt.v5.Mqtt5BlockingClient;
import plus.jmqx.client.mqtt.v5.Mqtt5ClientConfig;
import plus.jmqx.client.mqtt.v5.Mqtt5Publishes;
import plus.jmqx.client.mqtt.v5.Mqtt5RxClient;
import plus.jmqx.client.mqtt.v5.message.Mqtt5ConnAck;
import plus.jmqx.client.mqtt.v5.message.Mqtt5Publish;
import plus.jmqx.client.mqtt.v5.message.Mqtt5SubAck;
import plus.jmqx.client.mqtt.v5.message.Mqtt5Subscribe;
import plus.jmqx.client.mqtt.v5.message.Mqtt5Unsubscribe;
import reactor.core.Disposable;

import java.time.Duration;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * {@link Mqtt5BlockingClient} 的同步实现。
 *
 * @author maxid
 */
public class Mqtt5BlockingClientImpl implements Mqtt5BlockingClient {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    private final Mqtt5RxClient rx;

    public Mqtt5BlockingClientImpl(Mqtt5RxClient rx) {
        this.rx = rx;
    }

    @Override
    public Mqtt5ConnAck connect() {
        return rx.connect().block(TIMEOUT);
    }

    @Override
    public Mqtt5SubAck subscribe(Mqtt5Subscribe subscribe) {
        return rx.subscribe(subscribe).block(TIMEOUT);
    }

    @Override
    public Mqtt5Publishes publishes(MqttGlobalPublishFilter filter) {
        LinkedBlockingQueue<Mqtt5Publish> queue = new LinkedBlockingQueue<>();
        Disposable sub = rx.publishes(filter).subscribe(queue::offer);
        return Mqtt5Publishes.fromQueue(queue, sub::dispose);
    }

    @Override
    public void publish(Mqtt5Publish publish) {
        var result = rx.publish(publish).block(TIMEOUT);
        if (result != null && result.getError() != null) {
            throw new RuntimeException("PUBLISH failed", result.getError());
        }
    }

    @Override
    public void unsubscribe(Mqtt5Unsubscribe unsubscribe) {
        rx.unsubscribe(unsubscribe).block(TIMEOUT);
    }

    @Override
    public void disconnect() {
        rx.disconnect().block(TIMEOUT);
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
        return rx.toAsync();
    }

    @Override
    public Mqtt5RxClient toRx() {
        return rx;
    }

}
