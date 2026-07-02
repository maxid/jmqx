package plus.jmqx.client.mqtt.message;

/**
 * 消息构建辅助工具。
 *
 * @author maxid
 * @since 1.4.14
 */
public final class MqttMessageBuilder {

    /**
     * 工具类，禁止实例化
     */
    private MqttMessageBuilder() {
    }

    /**
     * 空安全地将 payload 复制到全新字节数组（不可变值类型语义）。
     *
     * @param src 源字节数组；{@code null} 时返回空数组
     * @return 副本
     */
    public static byte[] cloneBytes(byte[] src) {
        return src == null ? new byte[0] : src.clone();
    }

    /**
     * @param payload 字节数组；{@code null} 时视为 0 长度
     * @return payload 字节长度
     */
    public static int payloadSize(byte[] payload) {
        return payload == null ? 0 : payload.length;
    }

}
