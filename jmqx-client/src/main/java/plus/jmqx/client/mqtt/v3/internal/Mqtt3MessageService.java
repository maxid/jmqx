package plus.jmqx.client.mqtt.v3.internal;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import plus.jmqx.client.mqtt.MqttClientConfig;
import plus.jmqx.client.mqtt.internal.MqttMessageService;
import plus.jmqx.client.mqtt.message.MqttConnAck;
import plus.jmqx.client.mqtt.message.MqttPublish;
import plus.jmqx.client.mqtt.message.MqttSubAck;
import plus.jmqx.client.mqtt.message.MqttSubscribe;
import plus.jmqx.client.mqtt.message.MqttUnsubscribe;
import plus.jmqx.client.mqtt.message.QoS;
import plus.jmqx.client.mqtt.v3.message.Mqtt3ConnAck;
import plus.jmqx.client.mqtt.v3.message.Mqtt3ConnAckReturnCode;
import plus.jmqx.client.mqtt.v3.message.Mqtt3PublishImpl;
import plus.jmqx.client.mqtt.v3.message.Mqtt3SubAck;

import java.util.ArrayList;
import java.util.List;

/**
 * MQTT 3.1.1 协议适配器。
 *
 * <p>使用 netty-codec-mqtt（{@code MqttEncoder}/{@code MqttDecoder}/{@code MqttMessageBuilders}）
 * 处理线路格式，在业务消息类型与 Netty 类型之间进行转换。
 *
 * @author maxid
 */
public class Mqtt3MessageService implements MqttMessageService {

    /**
     * Netty MQTT 3.1.1 协议版本常量
     */
    private static final io.netty.handler.codec.mqtt.MqttVersion NETTY_VERSION =
            io.netty.handler.codec.mqtt.MqttVersion.MQTT_3_1_1;

    @Override
    public MqttMessage encodeConnect(MqttClientConfig config) {
        String clientId = config.getClientId() != null ? config.getClientId() : "";
        boolean hasWill = config.getWillPublish() != null;
        boolean hasPassword = config.getPassword() != null;
        boolean hasUsername = config.getUsername() != null;
        var builder = MqttMessageBuilders.connect()
                .protocolVersion(NETTY_VERSION)
                .clientId(clientId)
                .cleanSession(config.isCleanSession())
                .keepAlive(config.getKeepAliveSeconds())
                .hasUser(hasUsername)
                .hasPassword(hasPassword);
        if (hasUsername) {
            builder.username(config.getUsername());
        }
        if (hasPassword) {
            builder.password(config.getPassword());
        }
        if (hasWill) {
            builder.willFlag(true)
                    .willTopic(config.getWillPublish().getTopic())
                    .willMessage(config.getWillPublish().getPayloadAsBytes())
                    .willQoS(MqttQoS.valueOf(config.getWillPublish().getQoS().value()))
                    .willRetain(config.getWillPublish().isRetain());
        } else {
            builder.willFlag(false);
        }
        return builder.build();
    }

    @Override
    public MqttMessage encodePublish(MqttPublish publish, int packetId, boolean dup) {
        ByteBuf payload = publish.getPayloadAsBytes() != null && publish.getPayloadAsBytes().length > 0
                ? Unpooled.wrappedBuffer(publish.getPayloadAsBytes())
                : Unpooled.EMPTY_BUFFER;
        MqttFixedHeader fixed = new MqttFixedHeader(
                MqttMessageType.PUBLISH,
                dup,
                MqttQoS.valueOf(publish.getQoS().value()),
                publish.isRetain(),
                0
        );
        MqttPublishVariableHeader var = new MqttPublishVariableHeader(publish.getTopic(), packetId);
        return new MqttPublishMessage(fixed, var, payload);
    }

    @Override
    public MqttMessage encodeSubscribe(MqttSubscribe subscribe) {
        var builder = MqttMessageBuilders.subscribe()
                .messageId(subscribe.getPacketId());
        for (var tf : subscribe.getTopicFilters()) {
            builder.addSubscription(MqttQoS.valueOf(tf.getQoS().value()), tf.getTopicFilter());
        }
        return builder.build();
    }

    @Override
    public MqttMessage encodeUnsubscribe(MqttUnsubscribe unsubscribe) {
        var builder = MqttMessageBuilders.unsubscribe()
                .messageId(unsubscribe.getPacketId());
        for (var tf : unsubscribe.getTopicFilters()) {
            builder.addTopicFilter(tf);
        }
        return builder.build();
    }

    @Override
    public MqttMessage encodePubAck(int packetId) {
        return MqttMessageBuilders.pubAck().packetId(packetId).build();
    }

    @Override
    public MqttMessage encodePubRec(int packetId) {
        MqttFixedHeader fixed = new MqttFixedHeader(MqttMessageType.PUBREC, false, MqttQoS.AT_MOST_ONCE, false, 0);
        return new MqttMessage(fixed, MqttMessageIdVariableHeader.from(packetId));
    }

    @Override
    public MqttMessage encodePubRel(int packetId) {
        MqttFixedHeader fixed = new MqttFixedHeader(MqttMessageType.PUBREL, false, MqttQoS.AT_LEAST_ONCE, false, 0);
        return new MqttMessage(fixed, MqttMessageIdVariableHeader.from(packetId));
    }

    @Override
    public MqttMessage encodePubComp(int packetId) {
        MqttFixedHeader fixed = new MqttFixedHeader(MqttMessageType.PUBCOMP, false, MqttQoS.AT_MOST_ONCE, false, 0);
        return new MqttMessage(fixed, MqttMessageIdVariableHeader.from(packetId));
    }

    @Override
    public MqttMessage encodeDisconnect() {
        MqttFixedHeader fixed = new MqttFixedHeader(MqttMessageType.DISCONNECT, false, MqttQoS.AT_MOST_ONCE, false, 0);
        return new MqttMessage(fixed);
    }

    @Override
    public MqttMessage encodePingReq() {
        MqttFixedHeader fixed = new MqttFixedHeader(MqttMessageType.PINGREQ, false, MqttQoS.AT_MOST_ONCE, false, 0);
        return new MqttMessage(fixed);
    }

    @Override
    public MqttConnAck decodeConnAck(MqttConnAckMessage msg, MqttClientConfig config) {
        Mqtt3ConnAckReturnCode code = Mqtt3ConnAckReturnCode.fromCode(
                msg.variableHeader().connectReturnCode().byteValue());
        return new Mqtt3ConnAck(msg.variableHeader().isSessionPresent(), code);
    }

    @Override
    public MqttPublish decodePublish(MqttPublishMessage msg) {
        MqttFixedHeader fixed = msg.fixedHeader();
        ByteBuf buf = msg.payload();
        byte[] payload = new byte[buf.readableBytes()];
        buf.getBytes(buf.readerIndex(), payload);
        return new Mqtt3PublishImpl(
                msg.variableHeader().topicName(),
                payload,
                QoS.fromValue(fixed.qosLevel().value()),
                fixed.isRetain(),
                fixed.isDup(),
                msg.variableHeader().packetId()
        );
    }

    @Override
    public MqttSubAck decodeSubAck(MqttSubAckMessage msg) {
        List<QoS> granted = new ArrayList<>();
        for (int code : msg.payload().grantedQoSLevels()) {
            granted.add(QoS.fromValue(code));
        }
        return new Mqtt3SubAck(granted, msg.variableHeader().messageId());
    }

    @Override
    public int decodePacketId(MqttMessage msg) {
        if (msg.variableHeader() instanceof MqttMessageIdVariableHeader id) {
            return id.messageId();
        }
        if (msg instanceof MqttPublishMessage pub) {
            return pub.variableHeader().packetId();
        }
        return 0;
    }

    @Override
    public boolean isConnectionAccepted(MqttConnAck ack) {
        return ((Mqtt3ConnAck) ack).getReturnCode().isAccepted();
    }

    @Override
    public RuntimeException connectionRefusedException(MqttConnAck ack) {
        Mqtt3ConnAckReturnCode code = ((Mqtt3ConnAck) ack).getReturnCode();
        return new RuntimeException("MQTT 连接被拒绝: " + code);
    }

}
