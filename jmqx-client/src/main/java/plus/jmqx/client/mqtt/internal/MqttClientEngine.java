package plus.jmqx.client.mqtt.internal;

import lombok.extern.slf4j.Slf4j;
import plus.jmqx.client.mqtt.MqttClientConfig;
import plus.jmqx.client.mqtt.MqttClientState;
import plus.jmqx.client.mqtt.MqttGlobalPublishFilter;
import plus.jmqx.client.mqtt.internal.buffer.MessageBuffer;
import plus.jmqx.client.mqtt.internal.handler.MqttClientHandler;
import plus.jmqx.client.mqtt.internal.reconnect.MqttAutoReconnect;
import plus.jmqx.client.mqtt.internal.transport.TransportFactory;
import plus.jmqx.client.mqtt.internal.NettyUtil;
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
import plus.jmqx.client.mqtt.message.MqttSubAck;
import plus.jmqx.client.mqtt.message.MqttSubscribe;
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

    /**
     * 客户端配置
     */
    protected final MqttClientConfig                     config;
    /**
     * 消息编解码服务（v3/v5 适配）
     */
    protected final MqttMessageService                   service;
    /**
     * packetId 生成器
     */
    protected final PacketIdManager                      packetIdManager   = new PacketIdManager();
    /**
     * ACK 跟踪器
     */
    protected final AckTracker                           ackTracker        = new AckTracker();
    /**
     * 订阅存储
     */
    protected final SubscriptionStore                    subscriptionStore = new SubscriptionStore();
    /**
     * 入站投递枢纽
     */
    protected final MqttInbox                            inbox;
    /**
     * 入站 QoS 状态机
     */
    protected final InboundQos                           inboundQos        = new InboundQos();
    /**
     * 出站 inflight 限制器
     */
    protected final MqttOutbox                           outbox;
    /**
     * 离线消息缓冲
     */
    protected final MessageBuffer                        messageBuffer;
    /**
     * 传输工厂
     */
    protected final TransportFactory                     transportFactory  = new TransportFactory();
    /**
     * 已连接监听器列表
     */
    protected final List<MqttClientConnectedListener>    connectedListeners;
    /**
     * 已断开监听器列表
     */
    protected final List<MqttClientDisconnectedListener> disconnectedListeners;
    /**
     * 自动重连
     */
    protected final MqttAutoReconnect                    autoReconnect;

    /**
     * 客户端当前状态
     */
    protected final    AtomicReference<MqttClientState> state =
            new AtomicReference<>(MqttClientState.DISCONNECTED);
    /**
     * reactor-netty 连接
     */
    protected volatile Connection                       connection;
    /**
     * MQTT 业务处理器
     */
    protected volatile MqttClientHandler                handler;

    /**
     * 构造版本无关的 MQTT 客户端引擎。
     *
     * @param config                客户端配置
     * @param connectedListeners    已连接监听器列表
     * @param disconnectedListeners 已断开监听器列表
     */
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

    /**
     * 创建消息编解码服务（v3/v5 分别实现）。
     *
     * @param cfg 客户端配置
     * @return 消息编解码服务实例
     */
    protected abstract MqttMessageService createService(MqttClientConfig cfg);

    /**
     * 构建重新订阅请求。
     *
     * @param filters  主题过滤器列表
     * @param packetId 分配的 packetId
     * @return 订阅消息
     */
    protected abstract MqttSubscribe buildResubscribe(List<MqttTopicFilter> filters, int packetId);

    /**
     * 复制订阅消息并设置指定的 packetId。
     *
     * @param subscribe 原始订阅消息
     * @param packetId  新的 packetId
     * @return 复制后的订阅消息
     */
    protected abstract MqttSubscribe copySubscribeWithPacketId(MqttSubscribe subscribe, int packetId);

    /**
     * 复制取消订阅消息并设置指定的 packetId。
     *
     * @param unsubscribe 原始取消订阅消息
     * @param packetId    新的 packetId
     * @return 复制后的取消订阅消息
     */
    protected abstract MqttUnsubscribe copyUnsubscribeWithPacketId(MqttUnsubscribe unsubscribe, int packetId);

    /**
     * 执行引擎连接。
     *
     * @return 连接成功时发出 CONNACK 的 Mono
     */
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
                                .ofType(io.netty.handler.codec.mqtt.MqttMessage.class)
                                .doOnError(this::onTransportError)
                                .subscribe(mqtt -> h.handleInbound(conn.channel(), mqtt));
                        conn.onDispose().subscribe(v ->
                                onTransportError(new RuntimeException("connection disposed")));
                        NettyUtil.writeAndFlush(conn.channel(), service.encodeConnect(config));
                        return ackSink.asMono();
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

    /**
     * 执行订阅。
     *
     * @param subscribe 订阅消息
     * @return 订阅确认 Mono
     */
    public Mono<MqttSubAck> engineSubscribe(MqttSubscribe subscribe) {
        return Mono.defer(() -> {
            if (state.get() != MqttClientState.CONNECTED) {
                return Mono.error(new IllegalStateException("Not connected"));
            }
            int pid = packetIdManager.nextPacketId();
            MqttSubscribe withPid = copySubscribeWithPacketId(subscribe, pid);
            Sinks.One<MqttSubAck> sink = Sinks.one();
            handler.registerSubAck(pid, sink);
            NettyUtil.writeAndFlush(connection.channel(), service.encodeSubscribe(withPid));
            return sink.asMono();
        });
    }

    /**
     * 获取订阅匹配的发布流。
     *
     * @param subscribe 订阅消息（含主题过滤器）
     * @return 匹配主题的发布 Flux
     */
    public Flux<MqttPublish> engineSubscribePublishes(MqttSubscribe subscribe) {
        return inbox.globalFlux()
                .filter(d -> matchesAny(subscribe, d.getTopic()))
                .doOnNext(MqttInbox.Deliverable::consume)
                .map(this::toPublishView);
    }

    /**
     * 获取全局发布流（按过滤器类型订阅）。
     *
     * @param filter 全局发布过滤器
     * @return 匹配的发布 Flux
     */
    public Flux<MqttPublish> enginePublishes(MqttGlobalPublishFilter filter) {
        return inbox.globalFlux()
                .doOnNext(MqttInbox.Deliverable::consume)
                .filter(d -> matchesGlobalFilter(filter, d))
                .map(this::toPublishView);
    }

    /**
     * 执行发布。
     *
     * @param publish 发布消息
     * @return 发布结果 Mono
     */
    public Mono<MqttPublishResult> enginePublish(MqttPublish publish) {
        return doPublish(publish);
    }

    /**
     * 执行实际发布逻辑（含 QoS 处理和离线缓冲）。
     *
     * @param publish 发布消息
     * @return 发布结果 Mono
     */
    protected Mono<MqttPublishResult> doPublish(MqttPublish publish) {
        return Mono.defer(() -> {
            if (state.get() == MqttClientState.CONNECTED) {
                if (publish.getQoS() == QoS.AT_MOST_ONCE) {
                    NettyUtil.writeAndFlush(connection.channel(),
                            service.encodePublish(publish, 0, false));
                    return Mono.just(new MqttPublishResultImpl(publish, null));
                }
                int pid = packetIdManager.nextPacketId();
                MqttPublish withPid = withPacketId(publish, pid);
                Sinks.One<MqttPublishResult> sink = Sinks.one();
                PendingOutbound po = new PendingOutbound(withPid, sink);
                ackTracker.register(pid, po);
                return outbox.acquire(pid)
                        .doOnSuccess(v -> NettyUtil.writeAndFlush(connection.channel(),
                                service.encodePublish(withPid, pid, false)))
                        .then(sink.asMono())
                        .doFinally(s -> outbox.release(pid));
            }
            if (state.get() == MqttClientState.DISCONNECTED && config.isAutomaticReconnect()) {
                return messageBuffer.offer(publish).cast(MqttPublishResult.class);
            }
            return Mono.error(new IllegalStateException("Client is " + state.get()));
        });
    }

    /**
     * 执行取消订阅。
     *
     * @param unsubscribe 取消订阅消息
     * @return 完成 Mono
     */
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
            NettyUtil.writeAndFlush(connection.channel(), service.encodeUnsubscribe(withPid));
            return sink.asMono();
        });
    }

    /**
     * 执行断开连接。
     *
     * @return 断开完成 Mono
     */
    public Mono<Void> engineDisconnect() {
        return Mono.defer(() -> {
            if (autoReconnect != null) {
                autoReconnect.stop();
            }
            state.set(MqttClientState.DISCONNECTING);
            if (connection != null) {
                NettyUtil.writeAndFlush(connection.channel(), service.encodeDisconnect());
                connection.dispose();
                state.set(MqttClientState.DISCONNECTED);
                return Mono.empty();
            }
            state.set(MqttClientState.DISCONNECTED);
            return Mono.empty();
        });
    }

    /**
     * 获取客户端当前状态。
     *
     * @return 客户端状态枚举
     */
    public MqttClientState getState() {
        return state.get();
    }

    /**
     * CONNACK 接收后的处理（如 MQTT 5 Receive Maximum 处理）。
     *
     * @param ack 连接确认消息
     */
    protected void afterConnAck(MqttConnAck ack) {
        if (ack instanceof Mqtt5ConnAck a5) {
            int receiveMax = a5.getProperties().getReceiveMaximum();
            if (receiveMax > 0) {
                outbox.setMaxPermits(receiveMax);
            }
        }
    }

    /**
     * 重连后重新订阅之前的所有主题。
     */
    protected void resubscribe() {
        var filters = subscriptionStore.snapshotFilters();
        if (filters.isEmpty()) {
            return;
        }
        int pid = packetIdManager.nextPacketId();
        MqttSubscribe sub = buildResubscribe(filters, pid);
        NettyUtil.writeAndFlush(connection.channel(), service.encodeSubscribe(sub));
    }

    /**
     * 传输错误处理。
     *
     * @param err 传输异常
     */
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

    /**
     * 通知所有已连接监听器。
     *
     * @param sessionPresent 是否存在之前会话
     */
    protected void notifyConnected(boolean sessionPresent) {
        MqttClientConnectedContext ctx = new MqttClientConnectedContext(config, sessionPresent);
        for (var l : connectedListeners) {
            l.onConnected(ctx);
        }
    }

    /**
     * 将 Deliverable 转换为 MqttPublish 视图。
     *
     * @param d 入站投递元素
     * @return 发布消息视图
     */
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

    /**
     * 保留 ack() 回调的入站 PUBLISH 视图。
     */
    protected static final class DeliverablePublishView implements MqttPublish {
        /**
         * 代理的入站投递元素
         */
        private final MqttInbox.Deliverable d;

        /**
         * 构造 DeliverablePublishView。
         *
         * @param d 入站投递元素
         */
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

        /**
         * 获取原始入站投递元素。
         *
         * @return 原始 Deliverable
         */
        public MqttInbox.Deliverable deliverable() {
            return d;
        }
    }

}
