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
     * @return {@link Mono}
     */
    Mono<Void> onConnect(ConnectMessage message);

    /**
     * 设备断开连接(MQTT客户端主动发送disconnect报文断开连接)
     *
     * @param message 设备断开连接消息
     * @return {@link Mono}
     */
    Mono<Void> onDisconnect(DisconnectMessage message);

    /**
     * 失去设备连接(由于任何原因断开连接，无伦是服务端原因还是客户端原因)
     *
     * @param message 失去设备连接消息
     * @return {@link Mono}
     */
    Mono<Void> onConnectionLost(ConnectionLostMessage message);

    /**
     * 消息发布
     *
     * @param message 发布消息
     * @return {@link Mono}
     */
    Mono<Void> onPublish(PublishMessage message);

}
