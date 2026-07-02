package plus.jmqx.client.mqtt.v3.internal;

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
import plus.jmqx.client.mqtt.v3.Mqtt3AsyncClient;
import plus.jmqx.client.mqtt.v3.Mqtt3BlockingClient;
import plus.jmqx.client.mqtt.v3.Mqtt3ClientConfig;
import plus.jmqx.client.mqtt.v3.Mqtt3PublishResult;
import plus.jmqx.client.mqtt.v3.Mqtt3RxClient;
import plus.jmqx.client.mqtt.v3.message.Mqtt3ConnAck;
import plus.jmqx.client.mqtt.v3.message.Mqtt3Publish;
import plus.jmqx.client.mqtt.v3.message.Mqtt3PublishBuilder;
import plus.jmqx.client.mqtt.v3.message.Mqtt3SubAck;
import plus.jmqx.client.mqtt.v3.message.Mqtt3Subscribe;
import plus.jmqx.client.mqtt.v3.message.Mqtt3TopicFilter;
import plus.jmqx.client.mqtt.v3.message.Mqtt3Unsubscribe;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

/**
 * MQTT 3.1.1 客户端引擎默认实现。
 *
 * <p>继承 {@link MqttClientEngine}，在其通用引擎之上提供 MQTT 3.1.1 特定类型映射，
 * 实现 {@link Mqtt3RxClient} Reactive API。
 *
 * @author maxid
 */
public class DefaultMqtt3Client extends MqttClientEngine implements Mqtt3RxClient {

    /**
     * 构造 DefaultMqtt3Client 实例。
     *
     * @param config                MQTT 3 客户端配置
     * @param connectedListeners    连接成功监听器列表
     * @param disconnectedListeners 断开连接监听器列表
     */
    public DefaultMqtt3Client(Mqtt3ClientConfig config,
                              List<MqttClientConnectedListener> connectedListeners,
                              List<MqttClientDisconnectedListener> disconnectedListeners) {
        super(config, connectedListeners, disconnectedListeners);
    }

    @Override
    protected MqttMessageService createService(MqttClientConfig cfg) {
        return new Mqtt3MessageService();
    }

    @Override
    protected MqttSubscribe buildResubscribe(List<MqttTopicFilter> filters, int packetId) {
        return Mqtt3Subscribe.builder()
                .topicFilters(filters.stream().map(f -> Mqtt3TopicFilter.builder()
                        .topicFilter(f.getTopicFilter()).qos(f.getQoS()).build()).toList())
                .packetId(packetId)
                .build();
    }

    @Override
    protected MqttSubscribe copySubscribeWithPacketId(MqttSubscribe subscribe, int packetId) {
        if (subscribe instanceof Mqtt3Subscribe s3) {
            return s3.toBuilder().packetId(packetId).build();
        }
        throw new IllegalArgumentException("期望 Mqtt3Subscribe 类型");
    }

    @Override
    protected MqttUnsubscribe copyUnsubscribeWithPacketId(MqttUnsubscribe unsubscribe, int packetId) {
        if (unsubscribe instanceof Mqtt3Unsubscribe u3) {
            return u3.toBuilder().packetId(packetId).build();
        }
        throw new IllegalArgumentException("期望 Mqtt3Unsubscribe 类型");
    }

    @Override
    public Mono<Mqtt3ConnAck> connect() {
        return engineConnect().cast(Mqtt3ConnAck.class);
    }

    @Override
    public Mono<Mqtt3SubAck> subscribe(Mqtt3Subscribe subscribe) {
        return engineSubscribe(subscribe).cast(Mqtt3SubAck.class);
    }

    @Override
    public Flux<Mqtt3Publish> subscribePublishes(Mqtt3Subscribe subscribe) {
        return engineSubscribePublishes(subscribe).map(this::toMqtt3);
    }

    @Override
    public Flux<Mqtt3Publish> publishes(MqttGlobalPublishFilter filter) {
        return enginePublishes(filter).map(this::toMqtt3);
    }

    @Override
    public Mono<Mqtt3PublishResult> publish(Mqtt3Publish publish) {
        return enginePublish(publish).map(Mqtt3PublishResultDelegate::new);
    }

    @Override
    public Mono<Void> unsubscribe(Mqtt3Unsubscribe unsubscribe) {
        return engineUnsubscribe(unsubscribe);
    }

    @Override
    public Mono<Void> disconnect() {
        return engineDisconnect();
    }

    @Override
    public Mqtt3ClientConfig getConfig() {
        return (Mqtt3ClientConfig) config;
    }

    @Override
    public MqttVersion getVersion() {
        return MqttVersion.MQTT_3_1_1;
    }

    @Override
    public Mqtt3AsyncClient toAsync() {
        return new Mqtt3AsyncClientImpl(this);
    }

    @Override
    public Mqtt3BlockingClient toBlock() {
        return new Mqtt3BlockingClientImpl(this);
    }

    /**
     * 将通用 {@link MqttPublish} 转换为 {@link Mqtt3Publish}。
     *
     * @param publish 通用 publish 对象
     * @return MQTT 3 publish 对象
     */
    private Mqtt3Publish toMqtt3(MqttPublish publish) {
        if (publish instanceof Mqtt3Publish p3) {
            return p3;
        }
        if (publish instanceof DeliverablePublishView view) {
            return new Mqtt3PublishDelegate(view.deliverable());
        }
        return Mqtt3Publish.builder()
                .topic(publish.getTopic())
                .payload(publish.getPayloadAsBytes())
                .qos(publish.getQoS())
                .retain(publish.isRetain())
                .dup(publish.isDup())
                .packetId(publish.getPacketId())
                .build();
    }

    /**
     * {@link MqttInbox.Deliverable} 的 {@link Mqtt3Publish} 委托实现。
     */
    private static final class Mqtt3PublishDelegate implements Mqtt3Publish {
        /**
         * 底层可投递消息
         */
        private final MqttInbox.Deliverable d;

        Mqtt3PublishDelegate(MqttInbox.Deliverable d) {
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
        public void ack() {
            d.ack();
        }

        @Override
        public Mqtt3PublishBuilder toBuilder() {
            return Mqtt3Publish.builder()
                    .topic(d.getTopic()).payload(d.getPayloadAsBytes()).qos(d.getQoS())
                    .retain(d.isRetain()).dup(d.isDup()).packetId(d.getPacketId());
        }
    }

    /**
     * {@link Mqtt3PublishResult} 的委托实现。
     */
    private static final class Mqtt3PublishResultDelegate implements Mqtt3PublishResult {
        /**
         * 底层发布结果
         */
        private final plus.jmqx.client.mqtt.message.MqttPublishResult result;

        Mqtt3PublishResultDelegate(plus.jmqx.client.mqtt.message.MqttPublishResult result) {
            this.result = result;
        }

        @Override
        public Mqtt3Publish getPublish() {
            return (Mqtt3Publish) result.getPublish();
        }

        @Override
        public Throwable getError() {
            return result.getError();
        }
    }

}
