package plus.jmqx.client.mqtt.internal;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import lombok.extern.slf4j.Slf4j;
import plus.jmqx.client.mqtt.message.MqttPublish;
import plus.jmqx.client.mqtt.message.QoS;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 入站 QoS 状态机（Broker → Client）。
 *
 * <p>对于 QoS1/2，ACK 不在收到时发送 —— 而是作为 {@link Runnable} 交给 {@link MqttInbox}，
 * 仅在下游订阅者消费消息时才调用。这正是让 MQTT 自身的流控充当背压的机制：
 * 慢的订阅者延迟 ACK，broker 暂停推送。
 *
 * @author maxid
 */
@Slf4j
public final class InboundQos {

    private final Set<Integer> pendingPubRel = ConcurrentHashMap.newKeySet();

    public void onInboundPublish(ChannelHandlerContext ctx, MqttPublishMessage nettyMsg,
                                 MqttMessageService service, MqttInbox inbox) {
        MqttPublish pub = service.decodePublish(nettyMsg);
        int packetId = pub.getPacketId();
        switch (pub.getQoS()) {
            case AT_MOST_ONCE -> inbox.deliver(pub, () -> {});
            case AT_LEAST_ONCE -> inbox.deliver(pub, ackOnce(ctx,
                    () -> ctx.writeAndFlush(service.encodePubAck(packetId))));
            case EXACTLY_ONCE -> {
                pendingPubRel.add(packetId);
                inbox.deliver(pub, ackOnce(ctx,
                        () -> ctx.writeAndFlush(service.encodePubRec(packetId))));
            }
        }
    }

    public void onInboundPubRel(ChannelHandlerContext ctx, MqttMessage msg, MqttMessageService service) {
        int pid = service.decodePacketId(msg);
        if (pendingPubRel.remove(pid)) {
            ctx.writeAndFlush(service.encodePubComp(pid));
            log.debug("PUBCOMP sent for packetId={}", pid);
        }
    }

    /** 保证 ack 动作仅触发一次，即使下游多次调用 ack()。 */
    private Runnable ackOnce(ChannelHandlerContext ctx, Runnable ack) {
        AtomicBoolean fired = new AtomicBoolean();
        return () -> {
            if (fired.compareAndSet(false, true)) {
                try {
                    ack.run();
                    ctx.flush();
                } catch (Exception e) {
                    log.warn("ack failed", e);
                }
            }
        };
    }
}
