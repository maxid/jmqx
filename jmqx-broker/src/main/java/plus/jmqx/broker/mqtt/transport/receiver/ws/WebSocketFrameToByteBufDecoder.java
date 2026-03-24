package plus.jmqx.broker.mqtt.transport.receiver.ws;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;

import java.util.List;

/**
 * Websocket 帧解码器
 *
 * @author maxid
 * @since 2025/4/14 15:44
 */
public class WebSocketFrameToByteBufDecoder extends MessageToMessageDecoder<BinaryWebSocketFrame> {

    /**
     * 解码 WebSocket 二进制帧为 ByteBuf。
     *
     * @param ctx 处理上下文
     * @param msg WebSocket 帧
     * @param out 输出列表
     * @throws Exception 解码异常
     */
    @Override
    protected void decode(ChannelHandlerContext ctx, BinaryWebSocketFrame msg, List<Object> out) throws Exception {
        ByteBuf buf = msg.content();
        buf.retain();
        out.add(buf);
    }

}
