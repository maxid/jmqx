package plus.jmqx.broker.mqtt.message;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.*;

import java.util.List;
import java.util.Map;

import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.*;

/**
 * MQTT 消息构建器
 *
 * @author maxid
 * @since 2025/4/15 16:10
 */
public class MqttMessageBuilder {

    private static MqttProperties genMqttProperties(Map<String, String> userPropertiesMap) {
        MqttProperties mqttProperties = null;
        if (userPropertiesMap != null) {
            mqttProperties = new MqttProperties();
            MqttProperties.UserProperties userProperties = new MqttProperties.UserProperties();
            for (Map.Entry<String, String> entry : userPropertiesMap.entrySet()) {
                userProperties.add(entry.getKey(), entry.getValue());
            }
            mqttProperties.add(userProperties);
        }
        return mqttProperties;
    }

    /**
     * 创建 PUBLISH 消息（带 MqttProperties）
     *
     * @param isDup     是否重复分发
     * @param qoS       MQTT 服务质量等级
     * @param isRetain  是否保留消息
     * @param messageId 消息 ID
     * @param topic     主题
     * @param message   消息体
     * @param properties MQTT 属性
     * @return PUBLISH 消息
     */
    public static MqttPublishMessage publishMessage(boolean isDup, MqttQoS qoS, boolean isRetain, int messageId, String topic, ByteBuf message, MqttProperties properties) {
        MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(MqttMessageType.PUBLISH, isDup, qoS, isRetain, 0);
        MqttPublishVariableHeader mqttPublishVariableHeader = new MqttPublishVariableHeader(topic, messageId, properties);
        MqttPublishMessage mqttPublishMessage = new MqttPublishMessage(mqttFixedHeader, mqttPublishVariableHeader, message);
        return mqttPublishMessage;
    }

    /**
     * 创建 PUBLISH 消息（带用户属性，不保留）
     *
     * @param isDup              是否重复分发
     * @param qoS                MQTT 服务质量等级
     * @param messageId          消息 ID
     * @param topic              主题
     * @param message            消息体
     * @param userPropertiesMap  用户属性映射
     * @return PUBLISH 消息
     */
    public static MqttPublishMessage publishMessage(boolean isDup, MqttQoS qoS, int messageId, String topic, ByteBuf message, Map<String, String> userPropertiesMap) {
        MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(MqttMessageType.PUBLISH, isDup, qoS, false, 0);
        MqttPublishVariableHeader mqttPublishVariableHeader = new MqttPublishVariableHeader(topic, messageId, genMqttProperties(userPropertiesMap));
        MqttPublishMessage mqttPublishMessage = new MqttPublishMessage(mqttFixedHeader, mqttPublishVariableHeader, message);
        return mqttPublishMessage;
    }

    /**
     * 创建 PUBLISH 消息（带用户属性和保留标志）
     *
     * @param isDup              是否重复分发
     * @param qoS                MQTT 服务质量等级
     * @param isRetain           是否保留消息
     * @param messageId          消息 ID
     * @param topic              主题
     * @param message            消息体
     * @param userPropertiesMap  用户属性映射
     * @return PUBLISH 消息
     */
    public static MqttPublishMessage publishMessage(boolean isDup, MqttQoS qoS, boolean isRetain, int messageId, String topic, ByteBuf message, Map<String, String> userPropertiesMap) {
        MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(MqttMessageType.PUBLISH, isDup, qoS, isRetain, 0);
        MqttPublishVariableHeader mqttPublishVariableHeader = new MqttPublishVariableHeader(topic, messageId, genMqttProperties(userPropertiesMap));
        MqttPublishMessage mqttPublishMessage = new MqttPublishMessage(mqttFixedHeader, mqttPublishVariableHeader, message);
        return mqttPublishMessage;
    }

    /**
     * 创建 PUBLISH 消息（无保留、无属性）
     *
     * @param isDup     是否重复分发
     * @param qoS       MQTT 服务质量等级
     * @param messageId 消息 ID
     * @param topic     主题
     * @param message   消息体
     * @return PUBLISH 消息
     */
    public static MqttPublishMessage publishMessage(boolean isDup, MqttQoS qoS, int messageId, String topic, ByteBuf message) {
        MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(MqttMessageType.PUBLISH, isDup, qoS, false, 0);
        MqttPublishVariableHeader mqttPublishVariableHeader = new MqttPublishVariableHeader(topic, messageId);
        MqttPublishMessage mqttPublishMessage = new MqttPublishMessage(mqttFixedHeader, mqttPublishVariableHeader, message);
        return mqttPublishMessage;
    }

    /**
     * 创建 PUBLISH 消息（带保留标志，无属性）
     *
     * @param isDup     是否重复分发
     * @param qoS       MQTT 服务质量等级
     * @param isRetain  是否保留消息
     * @param messageId 消息 ID
     * @param topic     主题
     * @param message   消息体
     * @return PUBLISH 消息
     */
    public static MqttPublishMessage publishMessage(boolean isDup, MqttQoS qoS, boolean isRetain, int messageId, String topic, ByteBuf message) {
        MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(MqttMessageType.PUBLISH, isDup, qoS, isRetain, 0);
        MqttPublishVariableHeader mqttPublishVariableHeader = new MqttPublishVariableHeader(topic, messageId);
        MqttPublishMessage mqttPublishMessage = new MqttPublishMessage(mqttFixedHeader, mqttPublishVariableHeader, message);
        return mqttPublishMessage;
    }

    /**
     * 创建 PUBACK 消息
     *
     * @param messageId 消息 ID
     * @return PUBACK 消息
     */
    public static MqttPubAckMessage publishAckMessage(int messageId) {
        return ackMessage(MqttMessageType.PUBACK, messageId, false);
    }

    /**
     * 创建 PUBACK 消息（带原因码）
     *
     * @param messageId  消息 ID
     * @param reasonCode 原因码
     * @return PUBACK 消息
     */
    public static MqttPubAckMessage publishAckMessage(int messageId, byte reasonCode) {
        return ackMessage(MqttMessageType.PUBACK, messageId, false, reasonCode);
    }

    /**
     * 创建 PUBREC 消息
     *
     * @param messageId 消息 ID
     * @return PUBREC 消息
     */
    public static MqttPubAckMessage publishRecMessage(int messageId) {
        return ackMessage(MqttMessageType.PUBREC, messageId, false);
    }

    /**
     * 创建 PUBREC 消息（带原因码）
     *
     * @param messageId  消息 ID
     * @param reasonCode 原因码
     * @return PUBREC 消息
     */
    public static MqttPubAckMessage publishRecMessage(int messageId, byte reasonCode) {
        return ackMessage(MqttMessageType.PUBREC, messageId, false, reasonCode);
    }

    /**
     * PUBREL 消息
     *
     * @param messageId 消息ID
     * @return PUBREL 消息
     */
    public static MqttPubAckMessage publishRelMessage(int messageId) {
        MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(MqttMessageType.PUBREL, false, MqttQoS.AT_LEAST_ONCE, false, 0x02);
        MqttMessageIdVariableHeader from = MqttMessageIdVariableHeader.from(messageId);
        return new MqttPubAckMessage(mqttFixedHeader, from);
    }

    /**
     * 创建 PUBCOMP 消息
     *
     * @param messageId 消息 ID
     * @return PUBCOMP 消息
     */
    public static MqttPubAckMessage publishCompMessage(int messageId) {
        return ackMessage(MqttMessageType.PUBCOMP, messageId, false);

    }

    private static MqttPubAckMessage ackMessage(MqttMessageType mqttMessageType, int messageId, boolean isRetain) {
        MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(mqttMessageType, false, MqttQoS.AT_MOST_ONCE, isRetain, 0x02);
        MqttMessageIdVariableHeader from = MqttMessageIdVariableHeader.from(messageId);
        return new MqttPubAckMessage(mqttFixedHeader, from);
    }

    private static MqttPubAckMessage ackMessage(MqttMessageType mqttMessageType, int messageId, boolean isRetain, byte reasonCode) {
        MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(mqttMessageType, false, MqttQoS.AT_MOST_ONCE, isRetain, 0x02);
        MqttPubReplyMessageVariableHeader from = new MqttPubReplyMessageVariableHeader(messageId, reasonCode, MqttProperties.NO_PROPERTIES);
        return new MqttPubAckMessage(mqttFixedHeader, from);
    }

    /**
     * 创建 SUBACK 消息
     *
     * @param messageId 消息 ID
     * @param qos       服务质量等级列表
     * @return SUBACK 消息
     */
    public static MqttSubAckMessage subAckMessage(int messageId, List<Integer> qos) {
        MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(MqttMessageType.SUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0);
        MqttMessageIdVariableHeader variableHeader = MqttMessageIdVariableHeader.from(messageId);
        MqttSubAckPayload payload = new MqttSubAckPayload(qos);
        return new MqttSubAckMessage(mqttFixedHeader, variableHeader, payload);
    }

    /**
     * 创建 UNSUBACK 消息
     *
     * @param messageId 消息 ID
     * @return UNSUBACK 消息
     */
    public static MqttUnsubAckMessage unsubAckMessage(int messageId) {
        MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(MqttMessageType.UNSUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0x02);
        MqttMessageIdVariableHeader variableHeader = MqttMessageIdVariableHeader.from(messageId);
        return new MqttUnsubAckMessage(mqttFixedHeader, variableHeader);
    }

    /**
     * 创建 CONNACK 消息
     *
     * @param connectReturnCode 连接返回码
     * @param version           MQTT 协议版本
     * @return CONNACK 消息
     */
    public static MqttConnAckMessage connectAckMessage(MqttConnectReturnCode connectReturnCode, byte version) {
        MqttProperties properties = MqttProperties.NO_PROPERTIES;
        if (MqttVersion.MQTT_5.protocolLevel() == version) {
            properties = new MqttProperties();
            // support retain msg
            properties.add(new MqttProperties.IntegerProperty(MqttProperties.MqttPropertyType.RETAIN_AVAILABLE.value(), 1));
            // don't support shared subscription
            properties.add(new MqttProperties.IntegerProperty(MqttProperties.MqttPropertyType.SHARED_SUBSCRIPTION_AVAILABLE.value(), 0));
            // mqtt3.0 error code transform
            switch (connectReturnCode) {
                case CONNECTION_REFUSED_IDENTIFIER_REJECTED:
                    connectReturnCode = CONNECTION_REFUSED_CLIENT_IDENTIFIER_NOT_VALID;
                    break;
                case CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION:
                    connectReturnCode = CONNECTION_REFUSED_UNSUPPORTED_PROTOCOL_VERSION;
                    break;
                case CONNECTION_REFUSED_SERVER_UNAVAILABLE:
                    connectReturnCode = CONNECTION_REFUSED_SERVER_UNAVAILABLE_5;
                    break;
                case CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD:
                    connectReturnCode = CONNECTION_REFUSED_BAD_USERNAME_OR_PASSWORD;
                    break;
                case CONNECTION_REFUSED_NOT_AUTHORIZED:
                    connectReturnCode = CONNECTION_REFUSED_NOT_AUTHORIZED_5;
                    break;

            }
        }
        MqttConnAckVariableHeader mqttConnAckVariableHeader = new MqttConnAckVariableHeader(connectReturnCode, false, properties);
        MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(
                MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 0X02);
        return new MqttConnAckMessage(mqttFixedHeader, mqttConnAckVariableHeader);
    }

    /**
     * 创建 SUBSCRIBE 消息
     *
     * @param messageId          消息 ID
     * @param topicSubscriptions 主题订阅列表
     * @return SUBSCRIBE 消息
     */
    public static MqttSubscribeMessage subMessage(int messageId, List<MqttTopicSubscription> topicSubscriptions) {
        MqttSubscribePayload mqttSubscribePayload = new MqttSubscribePayload(topicSubscriptions);
        MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(MqttMessageType.SUBSCRIBE, false, MqttQoS.AT_LEAST_ONCE, false, 0);
        MqttMessageIdVariableHeader mqttMessageIdVariableHeader = MqttMessageIdVariableHeader.from(messageId);
        return new MqttSubscribeMessage(mqttFixedHeader, mqttMessageIdVariableHeader, mqttSubscribePayload);
    }

    /**
     * 创建 UNSUBSCRIBE 消息
     *
     * @param messageId 消息 ID
     * @param topics    取消订阅的主题列表
     * @return UNSUBSCRIBE 消息
     */
    public static MqttUnsubscribeMessage unSubMessage(int messageId, List<String> topics) {
        MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(MqttMessageType.UNSUBSCRIBE, false, MqttQoS.AT_LEAST_ONCE, false, 0x02);
        MqttMessageIdVariableHeader variableHeader = MqttMessageIdVariableHeader.from(messageId);
        MqttUnsubscribePayload MqttUnsubscribeMessage = new MqttUnsubscribePayload(topics);
        return new MqttUnsubscribeMessage(mqttFixedHeader, variableHeader, MqttUnsubscribeMessage);
    }

    /**
     * 创建 CONNECT 消息
     *
     * @param clientId   客户端 ID
     * @param willTopic  遗嘱主题
     * @param willMessage 遗嘱消息
     * @param username   用户名
     * @param password   密码
     * @param isUsername 是否包含用户名
     * @param isPassword 是否包含密码
     * @param isWill     是否包含遗嘱
     * @param willQos    遗嘱消息 QoS
     * @param heart      心跳间隔（秒）
     * @return CONNECT 消息
     */
    public static MqttConnectMessage connectMessage(String clientId, String willTopic, String willMessage, String username, String password, boolean isUsername, boolean isPassword, boolean isWill, int willQos, int heart) {
        MqttConnectVariableHeader mqttConnectVariableHeader = new MqttConnectVariableHeader(MqttVersion.MQTT_3_1_1.protocolName(), MqttVersion.MQTT_3_1_1.protocolLevel(), isUsername, isPassword, false, willQos, isWill, false, heart);
        MqttConnectPayload mqttConnectPayload = new MqttConnectPayload(clientId, willTopic, isWill ? willMessage.getBytes() : null, username, isPassword ? password.getBytes() : null);
        MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(MqttMessageType.CONNECT, false, MqttQoS.AT_MOST_ONCE, false, 10);
        return new MqttConnectMessage(mqttFixedHeader, mqttConnectVariableHeader, mqttConnectPayload);
    }

    /**
     * 创建 PINGREQ 消息
     *
     * @return PINGREQ 消息
     */
    public static MqttMessage pingMessage() {
        return new MqttMessage(new MqttFixedHeader(MqttMessageType.PINGREQ, false, MqttQoS.AT_MOST_ONCE, false, 0));
    }

    /**
     * PINGRESP 消息
     *
     * @return PINGRESP 消息
     */
    public static MqttMessage pongMessage() {
        return new MqttMessage(new MqttFixedHeader(MqttMessageType.PINGRESP, false, MqttQoS.AT_MOST_ONCE, false, 0));
    }

}
