package plus.jmqx.client.mqtt.internal;

import lombok.extern.slf4j.Slf4j;
import plus.jmqx.client.mqtt.MqttClientConfig;
import plus.jmqx.client.mqtt.MqttClientState;
import plus.jmqx.client.mqtt.MqttGlobalPublishFilter;
import plus.jmqx.client.mqtt.internal.buffer.MessageBuffer;
import plus.jmqx.client.mqtt.internal.handler.MqttClientHandler;
import plus.jmqx.client.mqtt.internal.reconnect.MqttAutoReconnect;
import plus.jmqx.client.mqtt.internal.transport.TransportFactory;
import plus.jmqx.client.mqtt.internal.util.PacketIdManager;
import plus.jmqx.client.mqtt.internal.util.TopicMatcher;
import plus.jmqx.client.mqtt.lifecycle.MqttClientConnectedContext;
import plus.jmqx.client.mqtt.lifecycle.MqttClientConnectedListener;
import plus.jmqx.client.mqtt.lifecycle.MqttClientDisconnectedContext;
import plus.jmqx.client.mqtt.lifecycle.MqttClientDisconnectedListener;
import plus.jmqx.client.mqtt.lifecycle.MqttClientReconnector;
import plus.jmqx.client.mqtt.message.MqttConnAck;
import plus.jmqx.client.mqtt.message.MqttPublish;
import plus.jmqx.client.mqtt.message.MqttPublishResult;
import plus.jmqx.client.mqtt.message.MqttSubscribe;
import plus.jmqx.client.mqtt.message.MqttSubAck;
import plus.jmqx.client.mqtt.message.MqttTopicFilter;
import plus.jmqx.client.mqtt.message.MqttUnsubscribe;
import plus.jmqx.client.mqtt.message.QoS;
import plus.jmqx.client.mqtt.v3.message.Mqtt3Publish;
import plus.jmqx.client.mqtt.v5.message.Mqtt5ConnAck;
import plus.jmqx.client.mqtt.v5.message.Mqtt5Publish;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.netty.Connection;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 版本无关的 MQTT 客户端引擎 —— reactor-netty 连接、QoS、背压、重连与离线缓存。
 *
 * <p>v3/v5 客户端通过子类提供协议适配与类型映射。
 *
 * @author maxid
 */
@Slf4j
public abstract class MqttClientEngine {

    protected final MqttClientConfig config;
    protected final MqttMessageService service;
    protected final PacketIdManager packetIdManager = new PacketIdManager();
    protected final AckTracker ackTracker = new AckTracker();
    protected final SubscriptionStore subscriptionStore = new SubscriptionStore();
    protected final MqttInbox inbox;
    protected final InboundQos inboundQos = new InboundQos();
    protected final MqttOutbox outbox;
    protected final MessageBuffer messageBuffer;
    protected final TransportFactory transportFactory = new TransportFactory();
    protected final List<MqttClientConnectedListener> connectedListeners;
    protected final List<MqttClientDisconnectedListener> disconnectedListeners;
    protected final MqttAutoReconnect autoReconnect;

    protected final AtomicReference<MqttClientState> state =
            new AtomicReference<>(MqttClientState.DISCONNECTED);
    protected volatile Connection connection;
    protected volatile MqttClientHandler handler;

    protected MqttClientEngine(MqttClientConfig config,
                               List<MqttClientConnectedListener> connectedListeners,
                               List<MqttClientDisconnectedListener> disconnectedListeners) {
        this.config = config;
        this.service = createService(config);
        this.inbox = new MqttInbox(config.getInboxBufferSize());
        this.outbox = new MqttOutbox(config.getMaxInflightMessages());
        this.messageBuffer = new MessageBuffer(config.getMessageBufferMaxSize(), config.getMessageBufferMaxBytes());
        this.connectedListeners = connectedListeners;
        this.disconnectedListeners = disconnectedListeners;
        this.autoReconnect = config.isAutomaticReconnect() ? new MqttAutoReconnect(
                config.getReconnectInitialDelayMs(), config.getReconnectMaxDelayMs(),
                () -> this.engineConnect().cast(Object.class),
                reactor.core.scheduler.Schedulers.parallel()) : null;
    }

    protected abstract MqttMessageService createService(MqttClientConfig cfg);

    protected abstract MqttSubscribe buildResubscribe(List<MqttTopicFilter> filters, int packetId);

    protected abstract MqttSubscribe copySubscribeWithPacketId(MqttSubscribe subscribe, int packetId);

    protected abstract MqttUnsubscribe copyUnsubscribeWithPacketId(MqttUnsubscribe unsubscribe, int packetId);

  protected Mono<MqttConnAck> engineConnect() {
        return Mono.defer(() -> {
            if (!state.compareAndSet(MqttClientState.DISCONNECTED, MqttClientState.CONNECTING)) {
                return Mono.error(new IllegalStateException("Client is " + state.get()));
            }
            Sinks.One<MqttConnAck> ackSink = Sinks.one();
            MqttClientHandler h = new MqttClientHandler(config, service, ackTracker, inbox, inboundQos, ackSink);
            return transportFactory.connect(config)
                    .flatMap(conn -> {
                        this.connection = conn;
                        this.handler = h;
                        transportFactory.installPipeline(conn, h, config);
                        conn.inbound().receiveObject()
                                .cast(io.netty.handler.codec.mqtt.MqttMessage.class)
                                .doOnError(this::onTransportError)
                                .subscribe();
                        conn.onDispose().subscribe(v ->
                                onTransportError(new RuntimeException("connection disposed")));
                        return conn.outbound().sendObject(Mono.just(service.encodeConnect(config))).then()
                                .then(ackSink.asMono());
                    })
                    .doOnSuccess(ack -> {
                        state.set(MqttClientState.CONNECTED);
                        afterConnAck(ack);
                        notifyConnected(ack.isSessionPresent());
                        resubscribe();
                        messageBuffer.flush(this::doPublish).subscribe();
                    })
                    .doOnError(err -> {
                        state.set(MqttClientState.DISCONNECTING);
                        onTransportError(err);
                    });
        });
    }

    public Mono<MqttSubAck> engineSubscribe(MqttSubscribe subscribe) {
        return Mono.defer(() -> {
            if (state.get() != MqttClientState.CONNECTED) {
                return Mono.error(new IllegalStateException("Not connected"));
            }
            int pid = packetIdManager.nextPacketId();
            MqttSubscribe withPid = copySubscribeWithPacketId(subscribe, pid);
            Sinks.One<MqttSubAck> sink = Sinks.one();
            handler.registerSubAck(pid, sink);
            return connection.outbound()
                    .sendObject(Mono.just(service.encodeSubscribe(withPid)))
                    .then()
                    .then(sink.asMono());
        });
    }

    public Flux<MqttPublish> engineSubscribePublishes(MqttSubscribe subscribe) {
        return inbox.globalFlux()
                .filter(d -> matchesAny(subscribe, d.getTopic()))
                .doOnNext(MqttInbox.Deliverable::consume)
                .map(this::toPublishView);
    }

    public Flux<MqttPublish> enginePublishes(MqttGlobalPublishFilter filter) {
        return inbox.globalFlux()
                .doOnNext(MqttInbox.Deliverable::consume)
                .filter(d -> matchesGlobalFilter(filter, d))
                .map(this::toPublishView);
    }

    public Mono<MqttPublishResult> enginePublish(MqttPublish publish) {
        return doPublish(publish);
    }

    protected Mono<MqttPublishResult> doPublish(MqttPublish publish) {
        return Mono.defer(() -> {
            if (state.get() == MqttClientState.CONNECTED) {
                if (publish.getQoS() == QoS.AT_MOST_ONCE) {
                    return connection.outbound()
                            .sendObject(Mono.just(service.encodePublish(publish, 0, false)))
                            .then()
                            .thenReturn(new MqttPublishResultImpl(publish, null));
                }
                int pid = packetIdManager.nextPacketId();
                MqttPublish withPid = withPacketId(publish, pid);
                Sinks.One<MqttPublishResult> sink = Sinks.one();
                PendingOutbound po = new PendingOutbound(withPid, sink);
                ackTracker.register(pid, po);
                return outbox.acquire(pid)
                        .then(connection.outbound()
                                .sendObject(Mono.just(service.encodePublish(withPid, pid, false)))
                                .then())
                        .then(sink.asMono())
                        .doFinally(s -> outbox.release(pid));
            }
            if (state.get() == MqttClientState.DISCONNECTED && config.isAutomaticReconnect()) {
                return messageBuffer.offer(publish).cast(MqttPublishResult.class);
            }
            return Mono.error(new IllegalStateException("Client is " + state.get()));
        });
    }

    public Mono<Void> engineUnsubscribe(MqttUnsubscribe unsubscribe) {
        return Mono.defer(() -> {
            if (state.get() != MqttClientState.CONNECTED) {
                return Mono.error(new IllegalStateException("Not connected"));
            }
            int pid = packetIdManager.nextPacketId();
            MqttUnsubscribe withPid = copyUnsubscribeWithPacketId(unsubscribe, pid);
            subscriptionStore.removeAll(withPid.getTopicFilters());
            Sinks.Empty<Void> sink = Sinks.empty();
            handler.registerUnsubAck(pid, sink);
            return connection.outbound()
                    .sendObject(Mono.just(service.encodeUnsubscribe(withPid)))
                    .then()
                    .then(sink.asMono());
        });
    }

    public Mono<Void> engineDisconnect() {
        return Mono.defer(() -> {
            if (autoReconnect != null) {
                autoReconnect.stop();
            }
            state.set(MqttClientState.DISCONNECTING);
            if (connection != null) {
                return connection.outbound().sendObject(Mono.just(service.encodeDisconnect())).then()
                        .doFinally(s -> {
                            connection.dispose();
                            state.set(MqttClientState.DISCONNECTED);
                        });
            }
            state.set(MqttClientState.DISCONNECTED);
            return Mono.empty();
        });
    }

    public MqttClientState getState() {
        return state.get();
    }

    protected void afterConnAck(MqttConnAck ack) {
        if (ack instanceof Mqtt5ConnAck a5) {
            int receiveMax = a5.getProperties().getReceiveMaximum();
            if (receiveMax > 0) {
                outbox.setMaxPermits(receiveMax);
            }
        }
    }

    protected void resubscribe() {
        var filters = subscriptionStore.snapshotFilters();
        if (filters.isEmpty()) {
            return;
        }
        int pid = packetIdManager.nextPacketId();
        MqttSubscribe sub = buildResubscribe(filters, pid);
        connection.outbound().sendObject(Mono.just(service.encodeSubscribe(sub))).then().subscribe();
    }

    protected void onTransportError(Throwable err) {
        log.warn("Transport error: {}", err.toString());
        state.set(MqttClientState.DISCONNECTED);
        MqttClientReconnector rc = new MqttClientReconnector(0, config.isAutomaticReconnect());
        MqttClientDisconnectedContext ctx = new MqttClientDisconnectedContext(
                config, MqttClientDisconnectedContext.DisconnectSource.SERVER, err, rc);
        for (var l : disconnectedListeners) {
            l.onDisconnected(ctx);
        }
        if (!config.isAutomaticReconnect()) {
            messageBuffer.failAll(err);
            ackTracker.failAll(err);
        }
    }

    protected void notifyConnected(boolean sessionPresent) {
        MqttClientConnectedContext ctx = new MqttClientConnectedContext(config, sessionPresent);
        for (var l : connectedListeners) {
            l.onConnected(ctx);
        }
    }

    protected MqttPublish toPublishView(MqttInbox.Deliverable d) {
        return new DeliverablePublishView(d);
    }

    private static MqttPublish withPacketId(MqttPublish publish, int pid) {
        if (publish instanceof Mqtt3Publish p3) {
            return p3.toBuilder().packetId(pid).build();
        }
        if (publish instanceof Mqtt5Publish p5) {
            return p5.toBuilder().packetId(pid).build();
        }
        throw new IllegalArgumentException("Unsupported publish type: " + publish.getClass());
    }

    private static boolean matchesAny(MqttSubscribe subscribe, String topic) {
        for (var tf : subscribe.getTopicFilters()) {
            if (TopicMatcher.matches(tf.getTopicFilter(), topic)) {
                return true;
            }
        }
        return false;
    }

    private static boolean matchesGlobalFilter(MqttGlobalPublishFilter filter, MqttInbox.Deliverable d) {
        return filter != MqttGlobalPublishFilter.UNSOLICITED || true;
    }

    /** 保留 ack() 回调的入站 PUBLISH 视图。 */
    protected static final class DeliverablePublishView implements MqttPublish {
        private final MqttInbox.Deliverable d;

        DeliverablePublishView(MqttInbox.Deliverable d) {
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

        public MqttInbox.Deliverable deliverable() {
            return d;
        }
    }
}
