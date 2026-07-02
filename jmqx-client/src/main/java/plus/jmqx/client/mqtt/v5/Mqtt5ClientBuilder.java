package plus.jmqx.client.mqtt.v5;

import plus.jmqx.client.mqtt.MqttClientConfig;
import plus.jmqx.client.mqtt.MqttVersion;
import plus.jmqx.client.mqtt.lifecycle.MqttClientConnectedListener;
import plus.jmqx.client.mqtt.lifecycle.MqttClientDisconnectedListener;
import plus.jmqx.client.mqtt.v5.internal.DefaultMqtt5Client;
import plus.jmqx.client.mqtt.v5.message.Mqtt5Publish;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * MQTT 5.0 客户端 builder。
 *
 * <p>API 风格对标 HiveMQ MQTT Client {@code Mqtt5ClientBuilder}。
 *
 * @author maxid
 * @since 1.4.14
 */
public class Mqtt5ClientBuilder {

    private final Mqtt5ClientConfig                    config       = new Mqtt5ClientConfig();
    private final List<MqttClientConnectedListener>    connected    = new ArrayList<>();
    private final List<MqttClientDisconnectedListener> disconnected = new ArrayList<>();

    /**
     * @param host MQTT broker 主机名或 IP。 @return this builder
     */
    public Mqtt5ClientBuilder serverHost(String host) {
        config.setServerHost(host);
        return this;
    }

    /**
     * @param port MQTT broker 端口。 @return this builder
     */
    public Mqtt5ClientBuilder serverPort(int port) {
        config.setServerPort(port);
        return this;
    }

    /**
     * @param clientId MQTT 客户端标识符。 @return this builder
     */
    public Mqtt5ClientBuilder identifier(String clientId) {
        config.setClientId(clientId);
        return this;
    }

    /**
     * 生成随机客户端标识符。 @return this builder
     */
    public Mqtt5ClientBuilder identifier() {
        config.setClientId("jmqx-" + UUID.randomUUID().toString().substring(0, 8));
        return this;
    }

    /**
     * @param seconds Keep Alive 间隔（秒）。 @return this builder
     */
    public Mqtt5ClientBuilder keepAliveSeconds(int seconds) {
        config.setKeepAliveSeconds(seconds);
        return this;
    }

    /**
     * @param cleanStart MQTT 5 cleanStart 标志。 @return this builder
     */
    public Mqtt5ClientBuilder cleanStart(boolean cleanStart) {
        config.setCleanSession(cleanStart);
        return this;
    }

    /**
     * @param seconds 会话过期时间（秒）。 @return this builder
     */
    public Mqtt5ClientBuilder sessionExpiryInterval(long seconds) {
        config.setSessionExpiryInterval(seconds);
        return this;
    }

    /**
     * @param receiveMaximum CONNECT 中声明的 Receive Maximum。 @return this builder
     */
    public Mqtt5ClientBuilder receiveMaximum(int receiveMaximum) {
        config.setReceiveMaximum(receiveMaximum);
        return this;
    }

    /**
     * @param username 认证用户名。 @return this builder
     */
    public Mqtt5ClientBuilder username(String username) {
        config.setUsername(username);
        return this;
    }

    /**
     * @param password 认证密码。 @return this builder
     */
    public Mqtt5ClientBuilder password(byte[] password) {
        config.setPassword(password);
        return this;
    }

    /**
     * @param willPublish 遗嘱消息。 @return this builder
     */
    public Mqtt5ClientBuilder willPublish(Mqtt5Publish willPublish) {
        config.setWillPublish(willPublish);
        return this;
    }

    /**
     * 启用自动重连。 @return this builder
     */
    public Mqtt5ClientBuilder automaticReconnect() {
        config.setAutomaticReconnect(true);
        return this;
    }

    /**
     * @param listener 连接成功监听器。 @return this builder
     */
    public Mqtt5ClientBuilder addConnectedListener(MqttClientConnectedListener listener) {
        connected.add(listener);
        return this;
    }

    /**
     * @param listener 断开连接监听器。 @return this builder
     */
    public Mqtt5ClientBuilder addDisconnectedListener(MqttClientDisconnectedListener listener) {
        disconnected.add(listener);
        return this;
    }

    /**
     * @param type 传输层类型。 @return this builder
     */
    public Mqtt5ClientBuilder transportType(MqttClientConfig.TransportType type) {
        config.setTransportType(type);
        return this;
    }

    /**
     * 确保客户端标识符已设置，若未设置则自动生成。
     * 同时设置 MQTT 协议版本为 5.0。
     */
    private void ensureClientId() {
        if (config.getClientId() == null || config.getClientId().isEmpty()) {
            config.setClientId("jmqx-" + UUID.randomUUID().toString().substring(0, 8));
        }
        config.setVersion(MqttVersion.MQTT_5);
    }

    /**
     * @return Reactor API 客户端
     */
    public Mqtt5RxClient buildRx() {
        ensureClientId();
        return new DefaultMqtt5Client(config, connected, disconnected);
    }

    /**
     * @return CompletableFuture 异步 API 客户端
     */
    public Mqtt5AsyncClient buildAsync() {
        ensureClientId();
        return buildRx().toAsync();
    }

    /**
     * @return 阻塞 API 客户端
     */
    public Mqtt5BlockingClient buildBlocking() {
        ensureClientId();
        return buildRx().toBlock();
    }

}
