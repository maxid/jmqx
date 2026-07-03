package plus.jmqx.client.mqtt.internal.transport.ws;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;

import java.util.List;

/**
 * 将 WebSocket 二进制帧解码为 MQTT 可消费的 ByteBuf（与 jmqx-broker 对称）。
 */
public final class WebSocketFrameToByteBufDecoder extends MessageToMessageDecoder<BinaryWebSocketFrame> {

    @Override
    protected void decode(ChannelHandlerContext ctx, BinaryWebSocketFrame msg, List<Object> out) {
        ByteBuf buf = msg.content();
        buf.retain();
        out.add(buf);
    }

}
