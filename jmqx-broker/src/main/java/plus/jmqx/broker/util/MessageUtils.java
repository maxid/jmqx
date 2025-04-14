package plus.jmqx.broker.util;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.*;
import lombok.extern.slf4j.Slf4j;
import plus.jmqx.broker.mqtt.retry.Ack;

/**
 * MQTT 消息工具
 *
 * @author maxid
 * @since 2025/4/11 09:58
 */
@Slf4j
public class MessageUtils {

    /**
     * 安全释放 MQTT 消息缓存区内存
     *
     * @param mqttMessage MQTT 消息
     */
    public static void safeRelease(MqttMessage mqttMessage) {
        if (mqttMessage.payload() instanceof ByteBuf) {
            ByteBuf buf = ((ByteBuf) mqttMessage.payload());
            MessageUtils.safeRelease("mqttMessage", buf, buf.refCnt());
        }
    }

    /**
     * 安全释放 MQTT 消息缓存区内存
     *
     * @param mqttMessage MQTT 消息
     * @param count       释放字节数
     */
    public static void safeRelease(MqttMessage mqttMessage, Integer count) {
        if (mqttMessage.payload() instanceof ByteBuf) {
            ByteBuf buf = ((ByteBuf) mqttMessage.payload());
            MessageUtils.safeRelease("mqttMessage", buf, count);
        }
    }

    /**
     * 安全释放缓存区内存
     *
     * @param buf 缓存区
     */
    public static void safeRelease(ByteBuf buf) {
        MessageUtils.safeRelease("byteBuf", buf, buf.refCnt());
    }

    /**
     * 安全释放缓存区内存
     *
     * @param name  名称
     * @param buf   缓存区
     * @param count 释放字节数
     */
    public static void safeRelease(String name, ByteBuf buf, Integer count) {
        if (count > 0) {
            buf.release(count);
            if (log.isDebugEnabled()) {
                log.debug("netty success release {} {} count {} ", name, buf, count);
            }
        }
    }

    /**
     * 生成发布消息
     *
     * @param messageId 消息ID
     * @param message   {@link MqttPublishMessage} 发布消息
     * @param mqttQoS   {@link MqttQoS} MQTT QoS
     * @return {@link MqttPublishMessage} 发布消息
     */
    public static MqttPublishMessage wrapPublishMessage(MqttPublishMessage message, MqttQoS mqttQoS, int messageId) {
        MqttPublishVariableHeader srcVariableHeader = message.variableHeader();
        MqttFixedHeader srcFixedHeader = message.fixedHeader();
        MqttFixedHeader fixedHeader = new MqttFixedHeader(srcFixedHeader.messageType(), false, mqttQoS, false, srcFixedHeader.remainingLength());
        MqttPublishVariableHeader variableHeader = new MqttPublishVariableHeader(srcVariableHeader.topicName(), messageId, srcVariableHeader.properties());
        return new MqttPublishMessage(fixedHeader, variableHeader, message.payload().copy());

    }

    /**
     * 获取释放消息字节数组
     *
     * @param byteBuf 消息ByteBuf
     * @return 字节数组
     */
    public static byte[] copyReleaseByteBuf(ByteBuf byteBuf) {
        byte[] bytes = new byte[byteBuf.readableBytes()];
        byteBuf.readBytes(bytes);
        byteBuf.resetReaderIndex();
        return bytes;
    }

    /**
     * 获取释放消息字节数组
     *
     * @param byteBuf 消息ByteBuf
     * @return 字节数组
     */
    public static byte[] copyByteBuf(ByteBuf byteBuf) {
        byte[] bytes = new byte[byteBuf.readableBytes()];
        byteBuf.resetReaderIndex();
        byteBuf.readBytes(bytes);
        byteBuf.resetReaderIndex();
        return bytes;
    }
}
