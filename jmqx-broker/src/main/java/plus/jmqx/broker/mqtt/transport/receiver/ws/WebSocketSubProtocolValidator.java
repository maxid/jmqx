package plus.jmqx.broker.mqtt.transport.receiver.ws;

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;

/**
 * 强制校验 WebSocket 握手请求必须携带 {@code Sec-WebSocket-Protocol} 头，
 * 缺失则直接返回 400 并关闭连接。
 *
 * <p>MQTT over WebSocket 规范要求客户端声明子协议（{@code mqtt / mqttv3.1 / mqttv3.1.1}），
 * 部分第三方 broker（如 EMQX）也同样严格校验；增加此校验实现规范合规。
 *
 * @author maxid
 * @since 1.4.16
 */
public class WebSocketSubProtocolValidator extends ChannelInboundHandlerAdapter {

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof FullHttpRequest) {
            FullHttpRequest req = (FullHttpRequest) msg;
            if (isWebSocketUpgrade(req) && !req.headers().contains(HttpHeaderNames.SEC_WEBSOCKET_PROTOCOL)) {
                DefaultFullHttpResponse resp = new DefaultFullHttpResponse(
                        HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_REQUEST);
                resp.headers().set(HttpHeaderNames.CONTENT_LENGTH, 0);
                ctx.writeAndFlush(resp).addListener(ChannelFutureListener.CLOSE);
                return;
            }
        }
        super.channelRead(ctx, msg);
    }

    private static boolean isWebSocketUpgrade(FullHttpRequest req) {
        return "websocket".equalsIgnoreCase(req.headers().get(HttpHeaderNames.UPGRADE));
    }
}
