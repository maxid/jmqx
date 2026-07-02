package plus.jmqx.client.mqtt.internal;

/**
 * 入站 QoS1/2 ACK 门控（完整实现见 Task 16）。当前为桩。
 *
 * @author maxid
 */
public final class InboundQos {

    public void onInboundPublish(io.netty.channel.ChannelHandlerContext ctx,
                                  io.netty.handler.codec.mqtt.MqttPublishMessage nettyMsg,
                                  MqttMessageService service, MqttInbox inbox) {
        plus.jmqx.client.mqtt.message.MqttPublish pub = service.decodePublish(nettyMsg);
        switch (pub.getQoS()) {
            case AT_MOST_ONCE -> inbox.deliver(pub, () -> {});
            case AT_LEAST_ONCE -> inbox.deliver(pub, () -> ctx.writeAndFlush(service.encodePubAck(pub.getPacketId())));
            case EXACTLY_ONCE -> inbox.deliver(pub, () -> ctx.writeAndFlush(service.encodePubRec(pub.getPacketId())));
        }
    }

    /** 处理入站 PUBREL（完整实现见 Task 16）。当前桩直接回 PUBCOMP。 */
    public void onInboundPubRel(io.netty.channel.ChannelHandlerContext ctx,
                                 io.netty.handler.codec.mqtt.MqttMessage msg,
                                 MqttMessageService service) {
        int pid = service.decodePacketId(msg);
        ctx.writeAndFlush(service.encodePubComp(pid));
    }
}
