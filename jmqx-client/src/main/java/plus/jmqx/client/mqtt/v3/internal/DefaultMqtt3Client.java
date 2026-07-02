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
 * MQTT 3.1.1 客户端引擎 —— 在 {@link MqttClientEngine} 之上提供 v3 类型映射。
 *
 * @author maxid
 */
public class DefaultMqtt3Client extends MqttClientEngine implements Mqtt3RxClient {

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
        throw new IllegalArgumentException("Expected Mqtt3Subscribe");
    }

    @Override
    protected MqttUnsubscribe copyUnsubscribeWithPacketId(MqttUnsubscribe unsubscribe, int packetId) {
        if (unsubscribe instanceof Mqtt3Unsubscribe u3) {
            return u3.toBuilder().packetId(packetId).build();
        }
        throw new IllegalArgumentException("Expected Mqtt3Unsubscribe");
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

    private static final class Mqtt3PublishDelegate implements Mqtt3Publish {
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

    private static final class Mqtt3PublishResultDelegate implements Mqtt3PublishResult {
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
