package plus.jmqx.client.mqtt.v5.internal;

import plus.jmqx.client.mqtt.MqttClientConfig;
import plus.jmqx.client.mqtt.MqttGlobalPublishFilter;
import plus.jmqx.client.mqtt.MqttVersion;
import plus.jmqx.client.mqtt.internal.MqttClientEngine;
import plus.jmqx.client.mqtt.internal.MqttInbox;
import plus.jmqx.client.mqtt.internal.MqttMessageService;
import plus.jmqx.client.mqtt.lifecycle.MqttClientConnectedListener;
import plus.jmqx.client.mqtt.lifecycle.MqttClientDisconnectedListener;
import plus.jmqx.client.mqtt.message.MqttPublish;
import plus.jmqx.client.mqtt.message.MqttSubscribe;
import plus.jmqx.client.mqtt.message.MqttTopicFilter;
import plus.jmqx.client.mqtt.message.MqttUnsubscribe;
import plus.jmqx.client.mqtt.message.QoS;
import plus.jmqx.client.mqtt.v5.Mqtt5AsyncClient;
import plus.jmqx.client.mqtt.v5.Mqtt5BlockingClient;
import plus.jmqx.client.mqtt.v5.Mqtt5ClientConfig;
import plus.jmqx.client.mqtt.v5.Mqtt5PublishResult;
import plus.jmqx.client.mqtt.v5.Mqtt5RxClient;
import plus.jmqx.client.mqtt.v5.message.Mqtt5ConnAck;
import plus.jmqx.client.mqtt.v5.message.Mqtt5Publish;
import plus.jmqx.client.mqtt.v5.message.Mqtt5PublishBuilder;
import plus.jmqx.client.mqtt.v5.message.Mqtt5PublishProperties;
import plus.jmqx.client.mqtt.v5.message.Mqtt5SubAck;
import plus.jmqx.client.mqtt.v5.message.Mqtt5Subscribe;
import plus.jmqx.client.mqtt.v5.message.Mqtt5TopicFilter;
import plus.jmqx.client.mqtt.v5.message.Mqtt5Unsubscribe;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

/**
 * MQTT 5.0 客户端引擎 —— 在 {@link MqttClientEngine} 之上提供 v5 类型映射。
 *
 * @author maxid
 */
public class DefaultMqtt5Client extends MqttClientEngine implements Mqtt5RxClient {

    /**
     * 构造 MQTT 5.0 客户端引擎实例。
     *
     * @param config                客户端配置
     * @param connectedListeners    连接成功监听器列表
     * @param disconnectedListeners 断开连接监听器列表
     */
    public DefaultMqtt5Client(Mqtt5ClientConfig config,
                              List<MqttClientConnectedListener> connectedListeners,
                              List<MqttClientDisconnectedListener> disconnectedListeners) {
        super(config, connectedListeners, disconnectedListeners);
    }

    /**
     * 创建 MQTT 5.0 消息服务适配器。
     *
     * @param cfg 客户端配置
     * @return MQTT 5 消息服务
     */
    @Override
    protected MqttMessageService createService(MqttClientConfig cfg) {
        return new Mqtt5MessageService();
    }

    /**
     * 构建重连时所需的订阅消息。
     *
     * @param filters  主题过滤器列表
     * @param packetId 报文标识符
     * @return MQTT 5 订阅消息
     */
    @Override
    protected MqttSubscribe buildResubscribe(List<MqttTopicFilter> filters, int packetId) {
        return Mqtt5Subscribe.builder()
                .topicFilters(filters.stream().map(f -> Mqtt5TopicFilter.builder()
                        .topicFilter(f.getTopicFilter()).qos(f.getQoS()).build()).toList())
                .packetId(packetId)
                .build();
    }

    /**
     * 使用新的报文标识符复制订阅消息。
     *
     * @param subscribe 原订阅消息
     * @param packetId  新的报文标识符
     * @return 复制后的订阅消息
     * @throws IllegalArgumentException 如果 subscribe 不是 Mqtt5Subscribe 实例
     */
    @Override
    protected MqttSubscribe copySubscribeWithPacketId(MqttSubscribe subscribe, int packetId) {
        if (subscribe instanceof Mqtt5Subscribe s5) {
            return s5.toBuilder().packetId(packetId).build();
        }
        throw new IllegalArgumentException("Expected Mqtt5Subscribe");
    }

    /**
     * 使用新的报文标识符复制取消订阅消息。
     *
     * @param unsubscribe 原取消订阅消息
     * @param packetId    新的报文标识符
     * @return 复制后的取消订阅消息
     * @throws IllegalArgumentException 如果 unsubscribe 不是 Mqtt5Unsubscribe 实例
     */
    @Override
    protected MqttUnsubscribe copyUnsubscribeWithPacketId(MqttUnsubscribe unsubscribe, int packetId) {
        if (unsubscribe instanceof Mqtt5Unsubscribe u5) {
            return u5.toBuilder().packetId(packetId).build();
        }
        throw new IllegalArgumentException("Expected Mqtt5Unsubscribe");
    }

    /**
     * 连接 MQTT broker。
     *
     * @return 携带 CONNACK 的 Mono
     */
    @Override
    public Mono<Mqtt5ConnAck> connect() {
        return engineConnect().cast(Mqtt5ConnAck.class);
    }

    /**
     * 向 broker 发送 SUBSCRIBE。
     *
     * @param subscribe 订阅消息
     * @return 携带 SUBACK 的 Mono
     */
    @Override
    public Mono<Mqtt5SubAck> subscribe(Mqtt5Subscribe subscribe) {
        return engineSubscribe(subscribe).cast(Mqtt5SubAck.class);
    }

    /**
     * 订阅并消费匹配主题过滤器的入站 PUBLISH。
     *
     * @param subscribe 订阅消息
     * @return 匹配的入站 publish 流
     */
    @Override
    public Flux<Mqtt5Publish> subscribePublishes(Mqtt5Subscribe subscribe) {
        return engineSubscribePublishes(subscribe).map(this::toMqtt5);
    }

    /**
     * 全局消费所有匹配给定过滤器的入站 PUBLISH。
     *
     * @param filter 入站消息过滤器
     * @return 入站 publish 流
     */
    @Override
    public Flux<Mqtt5Publish> publishes(MqttGlobalPublishFilter filter) {
        return enginePublishes(filter).map(this::toMqtt5);
    }

    /**
     * 发布一条 PUBLISH。
     *
     * @param publish 待发布的消息
     * @return 发布结果 Mono
     */
    @Override
    public Mono<Mqtt5PublishResult> publish(Mqtt5Publish publish) {
        return enginePublish(publish).map(Mqtt5PublishResultDelegate::new);
    }

    /**
     * 向 broker 发送 UNSUBSCRIBE。
     *
     * @param unsubscribe 取消订阅消息
     * @return 完成信号 Mono
     */
    @Override
    public Mono<Void> unsubscribe(Mqtt5Unsubscribe unsubscribe) {
        return engineUnsubscribe(unsubscribe);
    }

    /**
     * 发送 DISCONNECT 并关闭传输连接。
     *
     * @return 完成信号 Mono
     */
    @Override
    public Mono<Void> disconnect() {
        return engineDisconnect();
    }

    /**
     * 获取 MQTT 5.0 客户端配置。
     *
     * @return 客户端配置
     */
    @Override
    public Mqtt5ClientConfig getConfig() {
        return (Mqtt5ClientConfig) config;
    }

    /**
     * 获取 MQTT 协议版本。
     *
     * @return MQTT 5.0
     */
    @Override
    public MqttVersion getVersion() {
        return MqttVersion.MQTT_5;
    }

    /**
     * 转换为 CompletableFuture 异步 API。
     *
     * @return 异步客户端实现
     */
    @Override
    public Mqtt5AsyncClient toAsync() {
        return new Mqtt5AsyncClientImpl(this);
    }

    /**
     * 转换为阻塞 API。
     *
     * @return 阻塞客户端实现
     */
    @Override
    public Mqtt5BlockingClient toBlock() {
        return new Mqtt5BlockingClientImpl(this);
    }

    /**
     * 将通用 MqttPublish 转换为 Mqtt5Publish。
     *
     * @param publish 通用 publish 对象
     * @return MQTT 5 publish 对象
     */
    private Mqtt5Publish toMqtt5(MqttPublish publish) {
        if (publish instanceof Mqtt5Publish p5) {
            return p5;
        }
        if (publish instanceof DeliverablePublishView view) {
            return new Mqtt5PublishDelegate(view.deliverable());
        }
        return Mqtt5Publish.builder()
                .topic(publish.getTopic())
                .payload(publish.getPayloadAsBytes())
                .qos(publish.getQoS())
                .retain(publish.isRetain())
                .dup(publish.isDup())
                .packetId(publish.getPacketId())
                .properties(Mqtt5PublishProperties.builder().build())
                .build();
    }

    /**
     * Mqtt5Publish 委托实现，包装 {@link MqttInbox.Deliverable}。
     */
    private static final class Mqtt5PublishDelegate implements Mqtt5Publish {
        /**
         * 底层可投递对象
         */
        private final MqttInbox.Deliverable d;

        Mqtt5PublishDelegate(MqttInbox.Deliverable d) {
            this.d = d;
        }

        @Override
        public String getTopic() {
            return d.getTopic();
        }

        @Override
        public byte[] getPayloadAsBytes() {
            return d.getPayloadAsBytes();
        }

        @Override
        public QoS getQoS() {
            return d.getQoS();
        }

        @Override
        public boolean isRetain() {
            return d.isRetain();
        }

        @Override
        public boolean isDup() {
            return d.isDup();
        }

        @Override
        public int getPacketId() {
            return d.getPacketId();
        }

        @Override
        public Mqtt5PublishProperties getProperties() {
            return Mqtt5PublishProperties.builder().build();
        }

        @Override
        public void ack() {
            d.ack();
        }

        @Override
        public Mqtt5PublishBuilder toBuilder() {
            return Mqtt5Publish.builder()
                    .topic(d.getTopic()).payload(d.getPayloadAsBytes()).qos(d.getQoS())
                    .retain(d.isRetain()).dup(d.isDup()).packetId(d.getPacketId());
        }
    }

    /**
     * Mqtt5PublishResult 委托实现，包装通用发布结果。
     */
    private static final class Mqtt5PublishResultDelegate implements Mqtt5PublishResult {
        /**
         * 底层发布结果
         */
        private final plus.jmqx.client.mqtt.message.MqttPublishResult result;

        Mqtt5PublishResultDelegate(plus.jmqx.client.mqtt.message.MqttPublishResult result) {
            this.result = result;
        }

        @Override
        public Mqtt5Publish getPublish() {
            return (Mqtt5Publish) result.getPublish();
        }

        @Override
        public Throwable getError() {
            return result.getError();
        }
    }

}
