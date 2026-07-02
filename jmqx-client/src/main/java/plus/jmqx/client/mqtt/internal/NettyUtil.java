package plus.jmqx.client.mqtt.internal;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * Netty 缓冲区辅助工具。
 *
 * @author maxid
 */
public final class NettyUtil {

    private NettyUtil() {
    }

    public static ByteBuf wrap(byte[] bytes) {
        return bytes == null ? Unpooled.EMPTY_BUFFER : Unpooled.wrappedBuffer(bytes);
    }

}
