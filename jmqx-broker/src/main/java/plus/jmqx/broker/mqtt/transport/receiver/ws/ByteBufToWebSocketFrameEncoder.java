package plus.jmqx.broker.mqtt.transport.receiver.ws;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;

import java.util.List;

/**
 * Websocket 帧编码器
 *
 * @author maxid
 * @since 2025/4/14 15:40
 */
public class ByteBufToWebSocketFrameEncoder extends MessageToMessageEncoder<ByteBuf> {

    /**
     * 编码 ByteBuf 为 WebSocket 二进制帧
     *
     * @param ctx 处理上下文
     * @param msg 数据缓冲
     * @param out 输出列表
     * @throws Exception 编码异常
     */
    @Override
    protected void encode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) throws Exception {
        if (msg == null) {
            return;
        }
        BinaryWebSocketFrame result = new BinaryWebSocketFrame();
        result.content().writeBytes(msg);
        out.add(result);
    }

}
