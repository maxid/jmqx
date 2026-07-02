package plus.jmqx.client.mqtt.internal.handler;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import io.netty.handler.timeout.IdleStateEvent;
import lombok.extern.slf4j.Slf4j;
import plus.jmqx.client.mqtt.MqttClientConfig;
import plus.jmqx.client.mqtt.internal.AckTracker;
import plus.jmqx.client.mqtt.internal.InboundQos;
import plus.jmqx.client.mqtt.internal.MqttInbox;
import plus.jmqx.client.mqtt.internal.MqttMessageService;
import plus.jmqx.client.mqtt.internal.MqttPublishResultImpl;
import plus.jmqx.client.mqtt.message.MqttConnAck;
import plus.jmqx.client.mqtt.message.MqttSubAck;
import reactor.core.publisher.Sinks;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * MQTT 入站/出站分发的单一 Netty 处理器。
 *
 * <p>将所有逻辑委托给普通 Java 协作对象（service/tracker/inbox/inboundQos），这些对象可独立测试。
 *
 * @author maxid
 */
@Slf4j
public class MqttClientHandler extends ChannelDuplexHandler {

    private final MqttClientConfig       config;
    private final MqttMessageService     service;
    private final AckTracker             ackTracker;
    private final MqttInbox              inbox;
    private final InboundQos             inboundQos;
    private final Sinks.One<MqttConnAck> connAckSink;

    // 每个 packetId 的 SUBACK/UNSUBACK 完成槽
    private final Map<Integer, Sinks.One<MqttSubAck>> pendingSubAcks   = new ConcurrentHashMap<>();
    private final Map<Integer, Sinks.Empty<Void>>     pendingUnsubAcks = new ConcurrentHashMap<>();

    public MqttClientHandler(MqttClientConfig config,
                             MqttMessageService service,
                             AckTracker ackTracker,
                             MqttInbox inbox,
                             InboundQos inboundQos,
                             Sinks.One<MqttConnAck> connAckSink) {
        this.config = config;
        this.service = service;
        this.ackTracker = ackTracker;
        this.inbox = inbox;
        this.inboundQos = inboundQos;
        this.connAckSink = connAckSink;
    }

    public void registerSubAck(int packetId, Sinks.One<MqttSubAck> sink) {
        pendingSubAcks.put(packetId, sink);
    }

    public void registerUnsubAck(int packetId, Sinks.Empty<Void> sink) {
        pendingUnsubAcks.put(packetId, sink);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (!(msg instanceof MqttMessage mqtt)) {
            super.channelRead(ctx, msg);
            return;
        }
        MqttMessageType type = mqtt.fixedHeader() != null ? mqtt.fixedHeader().messageType() : null;
        if (type == null) {
            super.channelRead(ctx, msg);
            return;
        }
        log.warn("DIAG inbound type={}", type);

        switch (type) {
            case CONNACK -> {
                var ack = service.decodeConnAck((MqttConnAckMessage) mqtt, config);
                log.warn("DIAG CONNACK received: {}", type);
                connAckSink.tryEmitValue(ack);
            }
            case PUBLISH -> inboundQos.onInboundPublish(ctx, (MqttPublishMessage) mqtt, service, inbox);
            case PUBACK, PUBCOMP -> {
                int pid = service.decodePacketId(mqtt);
                ackTracker.complete(pid, new MqttPublishResultImpl(null, null));
            }
            case PUBREC -> {
                int pid = service.decodePacketId(mqtt);
                ackTracker.markReceived(pid);
                ctx.writeAndFlush(service.encodePubRel(pid));
            }
            case PUBREL -> inboundQos.onInboundPubRel(ctx, mqtt, service);
            case SUBACK -> {
                int pid = service.decodePacketId(mqtt);
                var sink = pendingSubAcks.remove(pid);
                if (sink != null) {
                    sink.tryEmitValue(service.decodeSubAck((MqttSubAckMessage) mqtt));
                }
            }
            case UNSUBACK -> {
                int pid = service.decodePacketId(mqtt);
                var sink = pendingUnsubAcks.remove(pid);
                if (sink != null) {
                    sink.tryEmitEmpty();
                }
            }
            case PINGRESP -> log.debug("PINGRESP received");
            case DISCONNECT -> log.debug("Server-initiated DISCONNECT");
            default -> log.debug("Unhandled MQTT message type: {}", type);
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            // 写 PINGREQ；读取空闲超过 1.5x keepalive 由独立 watch 处理
            ctx.writeAndFlush(service.encodePingReq());
        }
        super.userEventTriggered(ctx, evt);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Channel exception", cause);
        ctx.close();
    }

}
