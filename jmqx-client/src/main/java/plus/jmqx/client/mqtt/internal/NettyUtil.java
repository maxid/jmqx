package plus.jmqx.client.mqtt.internal;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

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

}
