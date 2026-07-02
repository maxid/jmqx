package plus.jmqx.client.mqtt.v3.internal;

import lombok.extern.slf4j.Slf4j;
import plus.jmqx.client.mqtt.MqttClientState;
import plus.jmqx.client.mqtt.MqttGlobalPublishFilter;
import plus.jmqx.client.mqtt.MqttVersion;
import plus.jmqx.client.mqtt.internal.AckTracker;
import plus.jmqx.client.mqtt.internal.InboundQos;
import plus.jmqx.client.mqtt.internal.MqttInbox;
import plus.jmqx.client.mqtt.internal.MqttMessageService;
import plus.jmqx.client.mqtt.internal.MqttOutbox;
import plus.jmqx.client.mqtt.internal.MqttPublishResultImpl;
import plus.jmqx.client.mqtt.internal.PendingOutbound;
import plus.jmqx.client.mqtt.internal.SubscriptionStore;
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
import reactor.core.publisher.Sinks;
import reactor.netty.Connection;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * MQTT 3.1.1 引擎 —— 单一 reactor-netty 引擎实现 {@link Mqtt3RxClient}。
 *
 * <p>组合所有协作对象（service/tracker/inbox/inboundQos/outbox/buffer/transport/reconnect）。
 * v5 通过继承并替换 {@link MqttMessageService} 扩展（Task 23）。
 *
 * <p>关键修复（vs 旧设计）：
 * <ul>
 *   <li>单一 PacketIdManager（消除双管理器碰撞）。</li>
 *   <li>MessageBuffer.flush 走完整 publish 路径（重注 ACK，不绕过 AckTracker）。</li>
 *   <li>双向背压：入站 request 门控 ACK；出站 inflight 信号量。</li>
 * </ul>
 *
 * @author maxid
 */
@Slf4j
public class DefaultMqtt3Client implements Mqtt3RxClient {

    private final Mqtt3ClientConfig config;
    private final MqttMessageService service;
    private final PacketIdManager packetIdManager = new PacketIdManager();
    private final AckTracker ackTracker = new AckTracker();
    private final SubscriptionStore subscriptionStore = new SubscriptionStore();
    private final MqttInbox inbox;
    private final InboundQos inboundQos = new InboundQos();
    private final MqttOutbox outbox;
    private final MessageBuffer messageBuffer;
    private final TransportFactory transportFactory = new TransportFactory();
    private final List<MqttClientConnectedListener> connectedListeners;
    private final List<MqttClientDisconnectedListener> disconnectedListeners;
    private final MqttAutoReconnect autoReconnect;

    private final AtomicReference<MqttClientState> state =
            new AtomicReference<>(MqttClientState.DISCONNECTED);
    private volatile Connection connection;
    private volatile MqttClientHandler handler;
    private final Sinks.One<MqttConnAck> connAckSink = Sinks.one();

    public DefaultMqtt3Client(Mqtt3ClientConfig config,
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
                () -> this.connect().cast(Object.class),
                reactor.core.scheduler.Schedulers.parallel()) : null;
    }

    /** 子类（v5）覆盖以返回 v5 协议适配器。 */
    protected MqttMessageService createService(Mqtt3ClientConfig cfg) {
        return new Mqtt3MessageService();
    }

    protected MqttMessageService service() {
        return service;
    }

    protected Mqtt3ClientConfig clientConfig() {
        return config;
    }

    protected Connection connection() {
        return connection;
    }

    @Override
    public Mono<Mqtt3ConnAck> connect() {
        return Mono.defer(() -> {
            if (!state.compareAndSet(MqttClientState.DISCONNECTED, MqttClientState.CONNECTING)) {
                return Mono.error(new IllegalStateException("Client is " + state.get()));
            }
            // 每次连接新建 handler 与 connAck sink
            Sinks.One<MqttConnAck> ackSink = Sinks.one();
            MqttClientHandler h = new MqttClientHandler(config, service, ackTracker, inbox, inboundQos, ackSink);
            return transportFactory.connect(config, c -> h)
                    .flatMap(conn -> {
                        this.connection = conn;
                        this.handler = h;
                        // 入站读取：reactor-netty 已通过 pipeline 自动分发到 handler，无需单独订阅 inbound
                        conn.onDispose().subscribe(v ->
                                onTransportError(new RuntimeException("connection disposed")));
                        return conn.outbound().sendObject(Mono.just(service.encodeConnect(config))).then()
                                .then(ackSink.asMono())
                                .map(ack -> (Mqtt3ConnAck) ack);
                    })
                    .doOnSuccess(ack -> {
                        state.set(MqttClientState.CONNECTED);
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

    @Override
    public Mono<Mqtt3SubAck> subscribe(Mqtt3Subscribe subscribe) {
        return Mono.defer(() -> {
            if (state.get() != MqttClientState.CONNECTED) {
                return Mono.error(new IllegalStateException("Not connected"));
            }
            int pid = packetIdManager.nextPacketId();
            Mqtt3Subscribe withPid = subscribe.toBuilder().packetId(pid).build();
            Sinks.One<plus.jmqx.client.mqtt.message.MqttSubAck> sink = Sinks.one();
            handler.registerSubAck(pid, sink);
            return connection.outbound()
                    .sendObject(Mono.just(service.encodeSubscribe(withPid)))
                    .then()
                    .then(sink.asMono().cast(Mqtt3SubAck.class));
        });
    }

    @Override
    public Flux<Mqtt3Publish> subscribePublishes(Mqtt3Subscribe subscribe) {
        return inbox.globalFlux()
                .filter(d -> matchesAny(subscribe, d.getTopic()))
                .doOnNext(MqttInbox.Deliverable::consume)
                .map(this::toMqtt3);
    }

    @Override
    public Flux<Mqtt3Publish> publishes(MqttGlobalPublishFilter filter) {
        return inbox.globalFlux()
                .doOnNext(MqttInbox.Deliverable::consume)
                .filter(d -> matchesGlobalFilter(filter, d))
                .map(this::toMqtt3);
    }

    /** 将 Deliverable 包装为保留 ack() 回调的 Mqtt3Publish。 */
    private Mqtt3Publish toMqtt3(MqttInbox.Deliverable d) {
        return new Mqtt3PublishDelegate(d);
    }

    @Override
    public Mono<Mqtt3PublishResult> publish(Mqtt3Publish publish) {
        return doPublish(publish).map(Mqtt3PublishResultDelegate::new);
    }

    /** 完整 publish 路径（被 MessageBuffer.flush 复用以重注 ACK）。 */
    protected Mono<MqttPublishResult> doPublish(MqttPublish publish) {
        return Mono.defer(() -> {
            if (state.get() == MqttClientState.CONNECTED) {
                if (publish.getQoS() == QoS.AT_MOST_ONCE) {
                    return connection.outbound()
                            .sendObject(Mono.just(service.encodePublish(publish, 0, false)))
                            .then()
                            .thenReturn((MqttPublishResult) new MqttPublishResultImpl(publish, null));
                }
                int pid = packetIdManager.nextPacketId();
                MqttPublish withPid = ((Mqtt3Publish) publish).toBuilder().packetId(pid).build();
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

    @Override
    public Mono<Void> unsubscribe(Mqtt3Unsubscribe unsubscribe) {
        return Mono.defer(() -> {
            if (state.get() != MqttClientState.CONNECTED) {
                return Mono.error(new IllegalStateException("Not connected"));
            }
            int pid = packetIdManager.nextPacketId();
            Mqtt3Unsubscribe withPid = unsubscribe.toBuilder().packetId(pid).build();
            subscriptionStore.removeAll(withPid.getTopicFilters());
            Sinks.Empty<Void> sink = Sinks.empty();
            handler.registerUnsubAck(pid, sink);
            return connection.outbound()
                    .sendObject(Mono.just(service.encodeUnsubscribe(withPid)))
                    .then()
                    .then(sink.asMono());
        });
    }

    @Override
    public Mono<Void> disconnect() {
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

    // --- helpers ---

    private void resubscribe() {
        var filters = subscriptionStore.snapshotFilters();
        if (filters.isEmpty()) {
            return;
        }
        int pid = packetIdManager.nextPacketId();
        Mqtt3Subscribe sub = Mqtt3Subscribe.builder()
                .topicFilters(filters.stream().map(f -> Mqtt3TopicFilter.builder()
                        .topicFilter(f.getTopicFilter()).qos(f.getQoS()).build()).toList())
                .packetId(pid).build();
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

    private void notifyConnected(boolean sessionPresent) {
        MqttClientConnectedContext ctx = new MqttClientConnectedContext(config, sessionPresent);
        for (var l : connectedListeners) {
            l.onConnected(ctx);
        }
    }

    private static boolean matchesAny(Mqtt3Subscribe subscribe, String topic) {
        for (var tf : subscribe.getTopicFilters()) {
            if (TopicMatcher.matches(tf.getTopicFilter(), topic)) {
                return true;
            }
        }
        return false;
    }

    private static boolean matchesGlobalFilter(MqttGlobalPublishFilter filter, MqttInbox.Deliverable d) {
        // 简化：ALL 全部；SUBSCRIBED/UNSOLICITED 由 SubscriptionStore 路由状态决定。
        // 当前 v1：ALL 全部，其余也全部（细分留待后续）。
        return filter != MqttGlobalPublishFilter.UNSOLICITED || true;
    }

    @Override
    public Mqtt3ClientConfig getConfig() {
        return config;
    }

    @Override
    public MqttClientState getState() {
        return state.get();
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

    /** Deliverable 的 Mqtt3Publish 委托视图，保留 ack()。 */
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

    /** {@link MqttPublishResult} 的 Mqtt3PublishResult 委托。 */
    private static final class Mqtt3PublishResultDelegate implements Mqtt3PublishResult {
        private final MqttPublishResult result;

        Mqtt3PublishResultDelegate(MqttPublishResult result) {
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
