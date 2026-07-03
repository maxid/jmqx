package plus.jmqx.client.mqtt.internal;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;

/**
 * Netty 缓冲区辅助工具。
 *
 * @author maxid
 */
public final class NettyUtil {

    /**
     * 工具类禁止实例化
     */
    private NettyUtil() {
    }

    /**
     * 将字节数组包装为 Netty ByteBuf。
     *
     * @param bytes 字节数组，可为 null
     * @return 包装后的 ByteBuf（null 输入返回空缓冲区）
     */
    public static ByteBuf wrap(byte[] bytes) {
        return bytes == null ? Unpooled.EMPTY_BUFFER : Unpooled.wrappedBuffer(bytes);
    }

    /**
     * 在 channel 所属 EventLoop 上写出并 flush 消息（与 jmqx-broker 测试客户端同构）。
     *
     * @param channel Netty channel
     * @param message 待写出对象（如 {@code MqttMessage}）
     */
    public static void writeAndFlush(Channel channel, Object message) {
        if (channel.eventLoop().inEventLoop()) {
            channel.writeAndFlush(message);
            return;
        }
        channel.eventLoop().execute(() -> channel.writeAndFlush(message));
    }

}
