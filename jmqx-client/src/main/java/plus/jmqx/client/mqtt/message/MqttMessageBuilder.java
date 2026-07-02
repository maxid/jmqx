package plus.jmqx.client.mqtt.message;

/**
 * 消息构建辅助工具。
 *
 * @author maxid
 */
public final class MqttMessageBuilder {

    private MqttMessageBuilder() {
    }

    /** 空安全地将 payload 复制到全新字节数组（不可变值类型语义）。 */
    public static byte[] cloneBytes(byte[] src) {
        return src == null ? new byte[0] : src.clone();
    }

    public static int payloadSize(byte[] payload) {
        return payload == null ? 0 : payload.length;
    }
}
