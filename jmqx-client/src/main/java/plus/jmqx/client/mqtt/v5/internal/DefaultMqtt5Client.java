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

    public DefaultMqtt5Client(Mqtt5ClientConfig config,
                              List<MqttClientConnectedListener> connectedListeners,
                              List<MqttClientDisconnectedListener> disconnectedListeners) {
        super(config, connectedListeners, disconnectedListeners);
    }

    @Override
    protected MqttMessageService createService(MqttClientConfig cfg) {
        return new Mqtt5MessageService();
    }

    @Override
    protected MqttSubscribe buildResubscribe(List<MqttTopicFilter> filters, int packetId) {
        return Mqtt5Subscribe.builder()
                .topicFilters(filters.stream().map(f -> Mqtt5TopicFilter.builder()
                        .topicFilter(f.getTopicFilter()).qos(f.getQoS()).build()).toList())
                .packetId(packetId)
                .build();
    }

    @Override
    protected MqttSubscribe copySubscribeWithPacketId(MqttSubscribe subscribe, int packetId) {
        if (subscribe instanceof Mqtt5Subscribe s5) {
            return s5.toBuilder().packetId(packetId).build();
        }
        throw new IllegalArgumentException("Expected Mqtt5Subscribe");
    }

    @Override
    protected MqttUnsubscribe copyUnsubscribeWithPacketId(MqttUnsubscribe unsubscribe, int packetId) {
        if (unsubscribe instanceof Mqtt5Unsubscribe u5) {
            return u5.toBuilder().packetId(packetId).build();
        }
        throw new IllegalArgumentException("Expected Mqtt5Unsubscribe");
    }

    @Override
    public Mono<Mqtt5ConnAck> connect() {
        return engineConnect().cast(Mqtt5ConnAck.class);
    }

    @Override
    public Mono<Mqtt5SubAck> subscribe(Mqtt5Subscribe subscribe) {
        return engineSubscribe(subscribe).cast(Mqtt5SubAck.class);
    }

    @Override
    public Flux<Mqtt5Publish> subscribePublishes(Mqtt5Subscribe subscribe) {
        return engineSubscribePublishes(subscribe).map(this::toMqtt5);
    }

    @Override
    public Flux<Mqtt5Publish> publishes(MqttGlobalPublishFilter filter) {
        return enginePublishes(filter).map(this::toMqtt5);
    }

    @Override
    public Mono<Mqtt5PublishResult> publish(Mqtt5Publish publish) {
        return enginePublish(publish).map(Mqtt5PublishResultDelegate::new);
    }

    @Override
    public Mono<Void> unsubscribe(Mqtt5Unsubscribe unsubscribe) {
        return engineUnsubscribe(unsubscribe);
    }

    @Override
    public Mono<Void> disconnect() {
        return engineDisconnect();
    }

    @Override
    public Mqtt5ClientConfig getConfig() {
        return (Mqtt5ClientConfig) config;
    }

    @Override
    public MqttVersion getVersion() {
        return MqttVersion.MQTT_5;
    }

    @Override
    public Mqtt5AsyncClient toAsync() {
        return new Mqtt5AsyncClientImpl(this);
    }

    @Override
    public Mqtt5BlockingClient toBlock() {
        return new Mqtt5BlockingClientImpl(this);
    }

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

    private static final class Mqtt5PublishDelegate implements Mqtt5Publish {
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

    private static final class Mqtt5PublishResultDelegate implements Mqtt5PublishResult {
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
