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
 * <p>内部委托给 {@link Mqtt5RxClient} 的 Reactor API，使用 block() 进行同步等待。
 *
 * @author maxid
 */
public class Mqtt5BlockingClientImpl implements Mqtt5BlockingClient {

    /** 默认操作超时时间 */
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    /** 被委托的 Reactor API 客户端 */
    private final Mqtt5RxClient rx;

    /**
     * 构造阻塞客户端实现。
     *
     * @param rx Reactor API 客户端
     */
    public Mqtt5BlockingClientImpl(Mqtt5RxClient rx) {
        this.rx = rx;
    }

    /**
     * 连接 MQTT broker。
     *
     * @return CONNACK
     */
    @Override
    public Mqtt5ConnAck connect() {
        return rx.connect().block(TIMEOUT);
    }

    /**
     * 向 broker 发送 SUBSCRIBE。
     *
     * @param subscribe 订阅消息
     * @return SUBACK
     */
    @Override
    public Mqtt5SubAck subscribe(Mqtt5Subscribe subscribe) {
        return rx.subscribe(subscribe).block(TIMEOUT);
    }

    /**
     * 获取阻塞式入站 publish 接收句柄。
     *
     * @param filter 入站消息过滤器
     * @return publish 接收句柄
     */
    @Override
    public Mqtt5Publishes publishes(MqttGlobalPublishFilter filter) {
        LinkedBlockingQueue<Mqtt5Publish> queue = new LinkedBlockingQueue<>();
        Disposable sub = rx.publishes(filter).subscribe(queue::offer);
        return Mqtt5Publishes.fromQueue(queue, sub::dispose);
    }

    /**
     * 发布一条 PUBLISH 消息。
     *
     * @param publish 待发布的消息
     * @throws RuntimeException 发布失败时抛出
     */
    @Override
    public void publish(Mqtt5Publish publish) {
        var result = rx.publish(publish).block(TIMEOUT);
        if (result != null && result.getError() != null) {
            throw new RuntimeException("PUBLISH failed", result.getError());
        }
    }

    /**
     * 向 broker 发送 UNSUBSCRIBE。
     *
     * @param unsubscribe 取消订阅消息
     */
    @Override
    public void unsubscribe(Mqtt5Unsubscribe unsubscribe) {
        rx.unsubscribe(unsubscribe).block(TIMEOUT);
    }

    /**
     * 断开连接。
     */
    @Override
    public void disconnect() {
        rx.disconnect().block(TIMEOUT);
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
     * 转换为异步 API。
     *
     * @return 异步客户端实现
     */
    @Override
    public Mqtt5AsyncClient toAsync() {
        return rx.toAsync();
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

}
