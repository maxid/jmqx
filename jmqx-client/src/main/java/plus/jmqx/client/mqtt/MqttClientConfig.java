package plus.jmqx.client.mqtt;

import plus.jmqx.client.mqtt.internal.transport.MqttSslConfig;
import plus.jmqx.client.mqtt.internal.transport.MqttWebSocketConfig;
import plus.jmqx.client.mqtt.message.MqttPublish;
import reactor.netty.resources.LoopResources;

/**
 * 版本无关的 MQTT 客户端配置基类。
 *
 * <p>由 {@link plus.jmqx.client.mqtt.v3.Mqtt3ClientConfig} 与
 * {@link plus.jmqx.client.mqtt.v5.Mqtt5ClientConfig} 继承；通常通过对应版本的
 * ClientBuilder 配置，而非直接实例化。
 *
 * @author maxid
 * @since 1.4.14
 */
public class MqttClientConfig {

    /**
     * 传输层类型。
     */
    public enum TransportType {
        /**
         * 明文 TCP。
         */
        TCP,
        /**
         * TLS 加密 TCP。
         */
        TLS,
        /**
         * WebSocket（明文）。
         */
        WS,
        /**
         * WebSocket over TLS。
         */
        WSS

    }

    /**
     * broker 主机名
     */
    private String              serverHost              = "localhost";
    /**
     * broker 端口
     */
    private int                 serverPort              = 1883;
    /**
     * 客户端标识符
     */
    private String              clientId;
    /**
     * Keep Alive 间隔（秒）
     */
    private int                 keepAliveSeconds        = 60;
    /**
     * MQTT 协议版本
     */
    private MqttVersion         version;
    /**
     * 套接字连接超时（毫秒）
     */
    private int                 socketConnectTimeoutMs  = 10_000;
    /**
     * MQTT CONNECT 握手超时（毫秒）
     */
    private int                 mqttConnectTimeoutMs    = 60_000;
    /**
     * 传输层类型
     */
    private TransportType       transportType           = TransportType.TCP;
    /**
     * TLS 配置
     */
    private MqttSslConfig       sslConfig;
    /**
     * WebSocket 配置
     */
    private MqttWebSocketConfig webSocketConfig;
    /**
     * Netty 事件循环线程数
     */
    private int                 nettyThreads            = Math.max(Runtime.getRuntime().availableProcessors(), 2);
    /**
     * 外部共享的事件循环资源
     */
    private LoopResources       loopResources;
    /**
     * cleanSession（v3）/ cleanStart（v5）标志
     */
    private boolean             cleanSession            = true;
    /**
     * 会话过期时间（秒，仅 v5）
     */
    private long                sessionExpiryInterval   = 0;
    /**
     * Receive Maximum（v5）
     */
    private int                 receiveMaximum          = 65535;
    /**
     * 认证用户名
     */
    private String              username;
    /**
     * 认证密码
     */
    private byte[]              password;
    /**
     * 遗嘱消息
     */
    private MqttPublish         willPublish;
    /**
     * 是否启用自动重连
     */
    private boolean             automaticReconnect      = false;
    /**
     * 重连初始延迟（毫秒）
     */
    private long                reconnectInitialDelayMs = 1000;
    /**
     * 重连最大延迟（毫秒）
     */
    private long                reconnectMaxDelayMs     = 120_000;
    /**
     * 最大重连次数
     */
    private int                 maxReconnectAttempts    = Integer.MAX_VALUE;
    /**
     * 断线缓存最大消息条数
     */
    private int                 messageBufferMaxSize    = 1000;
    /**
     * 断线缓存最大字节数
     */
    private long                messageBufferMaxBytes   = 64L * 1024 * 1024;
    /**
     * 断开时是否清空离线缓存
     */
    private boolean             clearBufferOnDisconnect = false;
    /**
     * 出站 inflight QoS1/2 消息上限
     */
    private int                 maxInflightMessages     = 64;
    /**
     * 入站消息背压缓冲区大小
     */
    private int                 inboxBufferSize         = 1024;

    /**
     * @return MQTT broker 主机名或 IP。
     */
    public String getServerHost() {
        return serverHost;
    }

    /**
     * @param serverHost MQTT broker 主机名或 IP。
     */
    public void setServerHost(String serverHost) {
        this.serverHost = serverHost;
    }

    /**
     * @return MQTT broker 端口。
     */
    public int getServerPort() {
        return serverPort;
    }

    /**
     * @param serverPort MQTT broker 端口。
     */
    public void setServerPort(int serverPort) {
        this.serverPort = serverPort;
    }

    /**
     * @return MQTT 客户端标识符；{@code null} 时由 builder 自动生成。
     */
    public String getClientId() {
        return clientId;
    }

    /**
     * @param clientId MQTT 客户端标识符。
     */
    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    /**
     * @return Keep Alive 间隔（秒）。
     */
    public int getKeepAliveSeconds() {
        return keepAliveSeconds;
    }

    /**
     * @param keepAliveSeconds Keep Alive 间隔（秒）。
     */
    public void setKeepAliveSeconds(int keepAliveSeconds) {
        this.keepAliveSeconds = keepAliveSeconds;
    }

    /**
     * @return 协商使用的 MQTT 协议版本。
     */
    public MqttVersion getVersion() {
        return version;
    }

    /**
     * @param version 协商使用的 MQTT 协议版本。
     */
    public void setVersion(MqttVersion version) {
        this.version = version;
    }

    /**
     * @return TCP 套接字连接超时（毫秒）。
     */
    public int getSocketConnectTimeoutMs() {
        return socketConnectTimeoutMs;
    }

    /**
     * @param ms TCP 套接字连接超时（毫秒）。
     */
    public void setSocketConnectTimeoutMs(int ms) {
        this.socketConnectTimeoutMs = ms;
    }

    /**
     * @return MQTT CONNECT 握手超时（毫秒）。
     */
    public int getMqttConnectTimeoutMs() {
        return mqttConnectTimeoutMs;
    }

    /**
     * @param ms MQTT CONNECT 握手超时（毫秒）。
     */
    public void setMqttConnectTimeoutMs(int ms) {
        this.mqttConnectTimeoutMs = ms;
    }

    /**
     * @return 传输层类型（TCP/TLS/WS/WSS）。
     */
    public TransportType getTransportType() {
        return transportType;
    }

    /**
     * @param transportType 传输层类型。
     */
    public void setTransportType(TransportType transportType) {
        this.transportType = transportType;
    }

    /**
     * @return TLS 配置；仅 {@link TransportType#TLS} / {@link TransportType#WSS} 时有效。
     */
    public MqttSslConfig getSslConfig() {
        return sslConfig;
    }

    /**
     * @param sslConfig TLS 配置。
     */
    public void setSslConfig(MqttSslConfig sslConfig) {
        this.sslConfig = sslConfig;
    }

    /**
     * @return WebSocket 配置；仅 {@link TransportType#WS} / {@link TransportType#WSS} 时有效。
     */
    public MqttWebSocketConfig getWebSocketConfig() {
        return webSocketConfig;
    }

    /**
     * @param webSocketConfig WebSocket 配置。
     */
    public void setWebSocketConfig(MqttWebSocketConfig webSocketConfig) {
        this.webSocketConfig = webSocketConfig;
    }

    /**
     * @return Netty 事件循环线程数。
     */
    public int getNettyThreads() {
        return nettyThreads;
    }

    /**
     * @param nettyThreads Netty 事件循环线程数。
     */
    public void setNettyThreads(int nettyThreads) {
        this.nettyThreads = nettyThreads;
    }

    /**
     * @return 外部共享的 {@link LoopResources}；{@code null} 时由客户端自行创建。
     */
    public LoopResources getLoopResources() {
        return loopResources;
    }

    /**
     * @param loopResources 外部共享的事件循环资源。
     */
    public void setLoopResources(LoopResources loopResources) {
        this.loopResources = loopResources;
    }

    /**
     * @return v3 {@code cleanSession} / v5 {@code cleanStart} 标志。
     */
    public boolean isCleanSession() {
        return cleanSession;
    }

    /**
     * @param cleanSession v3 cleanSession / v5 cleanStart。
     */
    public void setCleanSession(boolean cleanSession) {
        this.cleanSession = cleanSession;
    }

    /**
     * @return v5 会话过期时间（秒）；v3 忽略。
     */
    public long getSessionExpiryInterval() {
        return sessionExpiryInterval;
    }

    /**
     * @param sessionExpiryInterval v5 会话过期时间（秒）。
     */
    public void setSessionExpiryInterval(long sessionExpiryInterval) {
        this.sessionExpiryInterval = sessionExpiryInterval;
    }

    /**
     * @return v5 CONNECT 中声明的 Receive Maximum（客户端允许 broker 并发发送的 QoS1/2 消息上限）。
     */
    public int getReceiveMaximum() {
        return receiveMaximum;
    }

    /**
     * @param receiveMaximum v5 Receive Maximum。
     */
    public void setReceiveMaximum(int receiveMaximum) {
        this.receiveMaximum = receiveMaximum;
    }

    /**
     * @return 认证用户名；{@code null} 表示不使用用户名认证。
     */
    public String getUsername() {
        return username;
    }

    /**
     * @param username 认证用户名。
     */
    public void setUsername(String username) {
        this.username = username;
    }

    /**
     * @return 认证密码；{@code null} 表示不使用密码认证。
     */
    public byte[] getPassword() {
        return password;
    }

    /**
     * @param password 认证密码。
     */
    public void setPassword(byte[] password) {
        this.password = password;
    }

    /**
     * @return 遗嘱消息；{@code null} 表示不设置遗嘱。
     */
    public MqttPublish getWillPublish() {
        return willPublish;
    }

    /**
     * @param willPublish 遗嘱消息。
     */
    public void setWillPublish(MqttPublish willPublish) {
        this.willPublish = willPublish;
    }

    /**
     * @return 是否启用自动重连。
     */
    public boolean isAutomaticReconnect() {
        return automaticReconnect;
    }

    /**
     * @param automaticReconnect 是否启用自动重连。
     */
    public void setAutomaticReconnect(boolean automaticReconnect) {
        this.automaticReconnect = automaticReconnect;
    }

    /**
     * @return 自动重连初始延迟（毫秒）。
     */
    public long getReconnectInitialDelayMs() {
        return reconnectInitialDelayMs;
    }

    /**
     * @param ms 自动重连初始延迟（毫秒）。
     */
    public void setReconnectInitialDelayMs(long ms) {
        this.reconnectInitialDelayMs = ms;
    }

    /**
     * @return 自动重连最大延迟（毫秒）。
     */
    public long getReconnectMaxDelayMs() {
        return reconnectMaxDelayMs;
    }

    /**
     * @param ms 自动重连最大延迟（毫秒）。
     */
    public void setReconnectMaxDelayMs(long ms) {
        this.reconnectMaxDelayMs = ms;
    }

    /**
     * @return 最大重连尝试次数。
     */
    public int getMaxReconnectAttempts() {
        return maxReconnectAttempts;
    }

    /**
     * @param maxReconnectAttempts 最大重连尝试次数。
     */
    public void setMaxReconnectAttempts(int maxReconnectAttempts) {
        this.maxReconnectAttempts = maxReconnectAttempts;
    }

    /**
     * @return 断线缓存最大消息条数。
     */
    public int getMessageBufferMaxSize() {
        return messageBufferMaxSize;
    }

    /**
     * @param messageBufferMaxSize 断线缓存最大消息条数。
     */
    public void setMessageBufferMaxSize(int messageBufferMaxSize) {
        this.messageBufferMaxSize = messageBufferMaxSize;
    }

    /**
     * @return 断线缓存最大字节数。
     */
    public long getMessageBufferMaxBytes() {
        return messageBufferMaxBytes;
    }

    /**
     * @param messageBufferMaxBytes 断线缓存最大字节数。
     */
    public void setMessageBufferMaxBytes(long messageBufferMaxBytes) {
        this.messageBufferMaxBytes = messageBufferMaxBytes;
    }

    /**
     * @return 断开连接时是否清空离线缓存。
     */
    public boolean isClearBufferOnDisconnect() {
        return clearBufferOnDisconnect;
    }

    /**
     * @param clearBufferOnDisconnect 断开连接时是否清空离线缓存。
     */
    public void setClearBufferOnDisconnect(boolean clearBufferOnDisconnect) {
        this.clearBufferOnDisconnect = clearBufferOnDisconnect;
    }

    /**
     * @return 出站并发未 ACK 的 QoS1/2 消息上限（v3）；v5 可能被 CONNACK Receive Maximum 覆盖。
     */
    public int getMaxInflightMessages() {
        return maxInflightMessages;
    }

    /**
     * @param maxInflightMessages 出站 inflight 上限。
     */
    public void setMaxInflightMessages(int maxInflightMessages) {
        this.maxInflightMessages = maxInflightMessages;
    }

    /**
     * @return 入站消息背压缓冲区大小。
     */
    public int getInboxBufferSize() {
        return inboxBufferSize;
    }

    /**
     * @param inboxBufferSize 入站消息背压缓冲区大小。
     */
    public void setInboxBufferSize(int inboxBufferSize) {
        this.inboxBufferSize = inboxBufferSize;
    }

}
