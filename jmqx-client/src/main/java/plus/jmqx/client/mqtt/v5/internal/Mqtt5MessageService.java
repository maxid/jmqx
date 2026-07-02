package plus.jmqx.client.mqtt.v5.internal;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttProperties;
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
import plus.jmqx.client.mqtt.v5.message.Mqtt5ConnAck;
import plus.jmqx.client.mqtt.v5.message.Mqtt5ConnAckProperties;
import plus.jmqx.client.mqtt.v5.message.Mqtt5Publish;
import plus.jmqx.client.mqtt.v5.message.Mqtt5PublishImpl;
import plus.jmqx.client.mqtt.v5.message.Mqtt5PublishProperties;
import plus.jmqx.client.mqtt.v5.message.Mqtt5SubAck;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * MQTT 5.0 协议适配器。
 * <p>负责 MQTT 5 协议报文与业务对象之间的编码/解码。
 *
 * @author maxid
 */
public class Mqtt5MessageService implements MqttMessageService {

    /**
     * Netty MQTT 5 协议版本常量
     */
    private static final io.netty.handler.codec.mqtt.MqttVersion NETTY_VERSION =
            io.netty.handler.codec.mqtt.MqttVersion.MQTT_5;

    /**
     * 编码 CONNECT 报文。
     *
     * @param config 客户端配置
     * @return Netty MQTT CONNECT 报文
     */
    @Override
    public MqttMessage encodeConnect(MqttClientConfig config) {
        MqttProperties props = new MqttProperties();
        if (config.getSessionExpiryInterval() > 0) {
            props.add(new MqttProperties.IntegerProperty(
                    MqttProperties.MqttPropertyType.SESSION_EXPIRY_INTERVAL.value(),
                    (int) config.getSessionExpiryInterval()));
        }
        if (config.getReceiveMaximum() > 0 && config.getReceiveMaximum() < 65535) {
            props.add(new MqttProperties.IntegerProperty(
                    MqttProperties.MqttPropertyType.RECEIVE_MAXIMUM.value(),
                    config.getReceiveMaximum()));
        }

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
                .hasPassword(hasPassword)
                .properties(props);
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

    /**
     * 编码 PUBLISH 报文。
     *
     * @param publish  待发布的 publish 对象
     * @param packetId 报文标识符
     * @param dup      DUP 标志
     * @return Netty MQTT PUBLISH 报文
     */
    @Override
    public MqttMessage encodePublish(MqttPublish publish, int packetId, boolean dup) {
        ByteBuf payload = publish.getPayloadAsBytes() != null && publish.getPayloadAsBytes().length > 0
                ? Unpooled.wrappedBuffer(publish.getPayloadAsBytes())
                : Unpooled.EMPTY_BUFFER;
        MqttProperties props = new MqttProperties();
        if (publish instanceof Mqtt5Publish p5 && p5.getProperties() != null) {
            Mqtt5PublishProperties pp = p5.getProperties();
            if (pp.getMessageExpiryInterval() != null) {
                props.add(new MqttProperties.IntegerProperty(
                        MqttProperties.MqttPropertyType.PUBLICATION_EXPIRY_INTERVAL.value(),
                        pp.getMessageExpiryInterval()));
            }
            if (pp.getResponseTopic() != null) {
                props.add(new MqttProperties.StringProperty(
                        MqttProperties.MqttPropertyType.RESPONSE_TOPIC.value(),
                        pp.getResponseTopic()));
            }
            if (pp.getContentType() != null) {
                props.add(new MqttProperties.StringProperty(
                        MqttProperties.MqttPropertyType.CONTENT_TYPE.value(),
                        pp.getContentType()));
            }
            if (pp.getCorrelationData() != null) {
                props.add(new MqttProperties.BinaryProperty(
                        MqttProperties.MqttPropertyType.CORRELATION_DATA.value(),
                        pp.getCorrelationData()));
            }
            if (pp.getTopicAlias() != null) {
                props.add(new MqttProperties.IntegerProperty(
                        MqttProperties.MqttPropertyType.TOPIC_ALIAS.value(),
                        pp.getTopicAlias()));
            }
            if (pp.getUserProperties() != null && !pp.getUserProperties().isEmpty()) {
                for (Map.Entry<String, String> entry : pp.getUserProperties().entrySet()) {
                    props.add(new MqttProperties.UserProperty(entry.getKey(), entry.getValue()));
                }
            }
        }
        MqttFixedHeader fixed = new MqttFixedHeader(
                MqttMessageType.PUBLISH, dup,
                MqttQoS.valueOf(publish.getQoS().value()), publish.isRetain(), 0);
        MqttPublishVariableHeader var = new MqttPublishVariableHeader(publish.getTopic(), packetId, props);
        return new MqttPublishMessage(fixed, var, payload);
    }

    /**
     * 编码 SUBSCRIBE 报文。
     *
     * @param subscribe 订阅消息
     * @return Netty MQTT SUBSCRIBE 报文
     */
    @Override
    public MqttMessage encodeSubscribe(MqttSubscribe subscribe) {
        var builder = MqttMessageBuilders.subscribe()
                .messageId(subscribe.getPacketId())
                .properties(new MqttProperties());
        for (var tf : subscribe.getTopicFilters()) {
            builder.addSubscription(MqttQoS.valueOf(tf.getQoS().value()), tf.getTopicFilter());
        }
        return builder.build();
    }

    /**
     * 编码 UNSUBSCRIBE 报文。
     *
     * @param unsubscribe 取消订阅消息
     * @return Netty MQTT UNSUBSCRIBE 报文
     */
    @Override
    public MqttMessage encodeUnsubscribe(MqttUnsubscribe unsubscribe) {
        var builder = MqttMessageBuilders.unsubscribe()
                .messageId(unsubscribe.getPacketId());
        for (var tf : unsubscribe.getTopicFilters()) {
            builder.addTopicFilter(tf);
        }
        return builder.build();
    }

    /**
     * 编码 PUBACK 报文。
     *
     * @param packetId 报文标识符
     * @return Netty MQTT PUBACK 报文
     */
    @Override
    public MqttMessage encodePubAck(int packetId) {
        return MqttMessageBuilders.pubAck().packetId(packetId).build();
    }

    /**
     * 编码 PUBREC 报文。
     *
     * @param packetId 报文标识符
     * @return Netty MQTT PUBREC 报文
     */
    @Override
    public MqttMessage encodePubRec(int packetId) {
        MqttFixedHeader fixed = new MqttFixedHeader(MqttMessageType.PUBREC, false, MqttQoS.AT_MOST_ONCE, false, 0);
        return new MqttMessage(fixed, MqttMessageIdVariableHeader.from(packetId));
    }

    /**
     * 编码 PUBREL 报文。
     *
     * @param packetId 报文标识符
     * @return Netty MQTT PUBREL 报文
     */
    @Override
    public MqttMessage encodePubRel(int packetId) {
        MqttFixedHeader fixed = new MqttFixedHeader(MqttMessageType.PUBREL, false, MqttQoS.AT_LEAST_ONCE, false, 0);
        return new MqttMessage(fixed, MqttMessageIdVariableHeader.from(packetId));
    }

    /**
     * 编码 PUBCOMP 报文。
     *
     * @param packetId 报文标识符
     * @return Netty MQTT PUBCOMP 报文
     */
    @Override
    public MqttMessage encodePubComp(int packetId) {
        MqttFixedHeader fixed = new MqttFixedHeader(MqttMessageType.PUBCOMP, false, MqttQoS.AT_MOST_ONCE, false, 0);
        return new MqttMessage(fixed, MqttMessageIdVariableHeader.from(packetId));
    }

    /**
     * 编码 DISCONNECT 报文。
     *
     * @return Netty MQTT DISCONNECT 报文
     */
    @Override
    public MqttMessage encodeDisconnect() {
        MqttFixedHeader fixed = new MqttFixedHeader(MqttMessageType.DISCONNECT, false, MqttQoS.AT_MOST_ONCE, false, 0);
        return new MqttMessage(fixed);
    }

    /**
     * 编码 PINGREQ 报文。
     *
     * @return Netty MQTT PINGREQ 报文
     */
    @Override
    public MqttMessage encodePingReq() {
        MqttFixedHeader fixed = new MqttFixedHeader(MqttMessageType.PINGREQ, false, MqttQoS.AT_MOST_ONCE, false, 0);
        return new MqttMessage(fixed);
    }

    /**
     * 解码 CONNACK 报文为业务对象。
     *
     * @param msg    Netty CONNACK 报文
     * @param config 客户端配置（用于获取默认值）
     * @return MQTT 5 CONNACK 业务对象
     */
    @Override
    public MqttConnAck decodeConnAck(MqttConnAckMessage msg, MqttClientConfig config) {
        byte reasonCode = msg.variableHeader().connectReturnCode().byteValue();
        MqttProperties nettyProps = msg.variableHeader().properties();
        int receiveMax = getIntProperty(nettyProps, MqttProperties.MqttPropertyType.RECEIVE_MAXIMUM.value(), 65535);
        int serverKeepAlive = getIntProperty(nettyProps, MqttProperties.MqttPropertyType.SERVER_KEEP_ALIVE.value(),
                config.getKeepAliveSeconds());
        long sessionExpiry = getIntProperty(nettyProps, MqttProperties.MqttPropertyType.SESSION_EXPIRY_INTERVAL.value(), 0);
        String assignedId = getStringProperty(nettyProps, MqttProperties.MqttPropertyType.ASSIGNED_CLIENT_IDENTIFIER.value());
        Mqtt5ConnAckProperties properties = Mqtt5ConnAckProperties.builder()
                .receiveMaximum(receiveMax)
                .serverKeepAlive(serverKeepAlive)
                .sessionExpiryInterval(sessionExpiry)
                .assignedClientIdentifier(assignedId)
                .build();
        return new Mqtt5ConnAck(msg.variableHeader().isSessionPresent(), reasonCode, properties);
    }

    /**
     * 解码 PUBLISH 报文为业务对象。
     *
     * @param msg Netty PUBLISH 报文
     * @return MQTT 5 PUBLISH 业务对象
     */
    @Override
    public MqttPublish decodePublish(MqttPublishMessage msg) {
        MqttFixedHeader fixed = msg.fixedHeader();
        ByteBuf buf = msg.payload();
        byte[] payload = new byte[buf.readableBytes()];
        buf.getBytes(buf.readerIndex(), payload);
        Mqtt5PublishProperties.Mqtt5PublishPropertiesBuilder propsBuilder = Mqtt5PublishProperties.builder();
        MqttProperties nettyProps = msg.variableHeader().properties();
        if (nettyProps != null) {
            Integer expiry = getOptionalIntProperty(nettyProps,
                    MqttProperties.MqttPropertyType.PUBLICATION_EXPIRY_INTERVAL.value());
            if (expiry != null) {
                propsBuilder.messageExpiryInterval(expiry);
            }
            String responseTopic = getStringProperty(nettyProps,
                    MqttProperties.MqttPropertyType.RESPONSE_TOPIC.value());
            if (responseTopic != null) {
                propsBuilder.responseTopic(responseTopic);
            }
            String contentType = getStringProperty(nettyProps,
                    MqttProperties.MqttPropertyType.CONTENT_TYPE.value());
            if (contentType != null) {
                propsBuilder.contentType(contentType);
            }
            byte[] correlation = getBinaryProperty(nettyProps,
                    MqttProperties.MqttPropertyType.CORRELATION_DATA.value());
            if (correlation != null) {
                propsBuilder.correlationData(correlation);
            }
            Map<String, String> userProps = decodeUserProperties(nettyProps);
            if (!userProps.isEmpty()) {
                propsBuilder.userProperties(userProps);
            }
        }
        return new Mqtt5PublishImpl(
                msg.variableHeader().topicName(),
                payload,
                QoS.fromValue(fixed.qosLevel().value()),
                fixed.isRetain(),
                fixed.isDup(),
                msg.variableHeader().packetId(),
                propsBuilder.build());
    }

    /**
     * 解码 SUBACK 报文为业务对象。
     *
     * @param msg Netty SUBACK 报文
     * @return MQTT 5 SUBACK 业务对象
     */
    @Override
    public MqttSubAck decodeSubAck(MqttSubAckMessage msg) {
        List<Byte> reasons = new ArrayList<>();
        for (int code : msg.payload().reasonCodes()) {
            reasons.add((byte) code);
        }
        List<QoS> granted = new ArrayList<>();
        for (byte b : reasons) {
            if (b >= 0 && b <= 2) {
                granted.add(QoS.fromValue(b));
            }
        }
        return new Mqtt5SubAck(granted, reasons, msg.variableHeader().messageId());
    }

    /**
     * 从报文中解码报文标识符。
     *
     * @param msg MQTT 报文
     * @return 报文标识符，不存在则返回 0
     */
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

    /**
     * 判断连接是否被 broker 接受。
     *
     * @param ack CONNACK
     * @return true 表示连接被接受
     */
    @Override
    public boolean isConnectionAccepted(MqttConnAck ack) {
        return ((Mqtt5ConnAck) ack).isAccepted();
    }

    /**
     * 创建连接被拒绝时的异常。
     *
     * @param ack CONNACK
     * @return 连接被拒绝异常
     */
    @Override
    public RuntimeException connectionRefusedException(MqttConnAck ack) {
        return new RuntimeException("MQTT5 connection refused: reasonCode=" + ((Mqtt5ConnAck) ack).getReasonCode());
    }

    /**
     * 从 MQTT 属性中获取整型属性值（带默认值）。
     *
     * @param props MQTT 属性集合
     * @param type  属性类型标识
     * @param def   默认值
     * @return 属性值，不存在则返回默认值
     */
    private int getIntProperty(MqttProperties props, int type, int def) {
        if (props == null) {
            return def;
        }
        var p = props.getProperty(type);
        if (p instanceof MqttProperties.IntegerProperty ip) {
            return ip.value();
        }
        return def;
    }

    /**
     * 从 MQTT 属性中获取可选的整型属性值。
     *
     * @param props MQTT 属性集合
     * @param type  属性类型标识
     * @return 属性值，不存在则返回 null
     */
    private Integer getOptionalIntProperty(MqttProperties props, int type) {
        if (props == null) {
            return null;
        }
        var p = props.getProperty(type);
        if (p instanceof MqttProperties.IntegerProperty ip) {
            return ip.value();
        }
        return null;
    }

    /**
     * 从 MQTT 属性中获取字符串属性值。
     *
     * @param props MQTT 属性集合
     * @param type  属性类型标识
     * @return 属性值，不存在则返回 null
     */
    private String getStringProperty(MqttProperties props, int type) {
        if (props == null) {
            return null;
        }
        var p = props.getProperty(type);
        if (p instanceof MqttProperties.StringProperty sp) {
            return sp.value();
        }
        return null;
    }

    /**
     * 从 MQTT 属性中获取二进制属性值。
     *
     * @param props MQTT 属性集合
     * @param type  属性类型标识
     * @return 属性值，不存在则返回 null
     */
    private byte[] getBinaryProperty(MqttProperties props, int type) {
        if (props == null) {
            return null;
        }
        var p = props.getProperty(type);
        if (p instanceof MqttProperties.BinaryProperty bp) {
            return bp.value();
        }
        return null;
    }

    /**
     * 从 MQTT 属性中解码用户属性映射。
     *
     * @param props MQTT 属性集合
     * @return 用户属性键值对映射
     */
    private Map<String, String> decodeUserProperties(MqttProperties props) {
        Map<String, String> userProps = new HashMap<>();
        for (MqttProperties.MqttProperty property : props.listAll()) {
            if (property.propertyId() == MqttProperties.MqttPropertyType.USER_PROPERTY.value()
                    && property instanceof MqttProperties.UserProperty up) {
                MqttProperties.StringPair pair = up.value();
                userProps.put(pair.key, pair.value);
            }
        }
        return userProps;
    }

}
