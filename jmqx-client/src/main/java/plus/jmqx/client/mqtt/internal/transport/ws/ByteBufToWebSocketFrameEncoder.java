package plus.jmqx.client.mqtt.internal.transport.ws;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;

import java.util.List;

/**
 * 将 MQTT 编码后的 ByteBuf 封装为 WebSocket 二进制帧（与 jmqx-broker 对称）。
 */
public final class ByteBufToWebSocketFrameEncoder extends MessageToMessageEncoder<ByteBuf> {

    @Override
    protected void encode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) {
        if (msg == null) {
            return;
        }
        BinaryWebSocketFrame frame = new BinaryWebSocketFrame();
        frame.content().writeBytes(msg);
        out.add(frame);
    }

}
