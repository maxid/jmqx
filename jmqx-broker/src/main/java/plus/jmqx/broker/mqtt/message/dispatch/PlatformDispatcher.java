package plus.jmqx.broker.mqtt.message.dispatch;

import reactor.core.publisher.Mono;

/**
 * MQTT 除订阅、去订阅外的生命周开放接口
 *
 * @author maxid
 * @since 2025/4/21 14:41
 */
public interface PlatformDispatcher {

    /**
     * 设备连接
     *
     * @param message 设备连接消息
     * @return 处理结果
     */
    Mono<Void> onConnect(ConnectMessage message);

    /**
     * 设备断开连接(MQTT客户端主动发送disconnect报文断开连接)
     *
     * @param message 设备断开连接消息
     * @return 处理结果
     */
    Mono<Void> onDisconnect(DisconnectMessage message);

    /**
     * 失去设备连接(由于任何原因断开连接，无伦是服务端原因还是客户端原因)
     *
     * @param message 失去设备连接消息
     * @return 处理结果
     */
    Mono<Void> onConnectionLost(ConnectionLostMessage message);

    /**
     * 消息发布
     *
     * @param message 发布消息
     * @return 处理结果
     */
    Mono<Void> onPublish(PublishMessage message);

    /**
     * 设备心跳（MQTT PINGREQ）。默认空实现，避免已有分发器编译失败。
     * <p>
     * Broker 已在 EventLoop 上回 PONG；本回调在 {@code dispatchScheduler} 上执行。
     *
     * @param message 心跳消息（clientId / username）
     * @return 处理结果
     */
    default Mono<Void> onPing(PingMessage message) {
        return Mono.empty();
    }

}
