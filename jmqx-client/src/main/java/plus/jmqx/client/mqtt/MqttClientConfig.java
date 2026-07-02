package plus.jmqx.client.mqtt;

import plus.jmqx.client.mqtt.internal.transport.MqttSslConfig;
import plus.jmqx.client.mqtt.internal.transport.MqttWebSocketConfig;
import plus.jmqx.client.mqtt.message.MqttPublish;
import reactor.netty.resources.LoopResources;

/**
 * 版本无关的客户端配置基类。
 *
 * <p>v3 / v5 各有其子类（{@code Mqtt3ClientConfig} / {@code Mqtt5ClientConfig}）。
 *
 * @author maxid
 */
public class MqttClientConfig {

    public enum TransportType {
        TCP, TLS, WS, WSS
    }

    // 连接
    private String serverHost = "localhost";
    private int serverPort = 1883;
    private String clientId;                 // null -> 自动生成 "jmqx-<uuid8>"
    private int keepAliveSeconds = 60;
    private MqttVersion version;             // 由 builder.useMqttVersionX() 设置

    // 超时
    private int socketConnectTimeoutMs = 10_000;
    private int mqttConnectTimeoutMs = 60_000;

    // 传输
    private TransportType transportType = TransportType.TCP;
    private MqttSslConfig sslConfig;
    private MqttWebSocketConfig webSocketConfig;

    // 线程
    private int nettyThreads = Math.max(Runtime.getRuntime().availableProcessors(), 2);
    private LoopResources loopResources;     // 外部注入的共享 loop，可选

    // 会话
    private boolean cleanSession = true;      // v3；v5 映射为 cleanStart
    private long sessionExpiryInterval = 0;   // 仅 v5（秒）
    private int receiveMaximum = 65535;        // 仅 v5 —— 客户端对 server inflight 的上限

    // 认证
    private String username;
    private byte[] password;

    // 遗嘱
    private MqttPublish willPublish;

    // 重连
    private boolean automaticReconnect = false;
    private long reconnectInitialDelayMs = 1000;
    private long reconnectMaxDelayMs = 120_000;
    private int maxReconnectAttempts = Integer.MAX_VALUE;

    // 缓存与流控
    private int messageBufferMaxSize = 1000;
    private long messageBufferMaxBytes = 64L * 1024 * 1024;
    private boolean clearBufferOnDisconnect = false;
    private int maxInflightMessages = 64;      // v3；v5 由 CONNACK Receive Maximum 覆盖
    private int inboxBufferSize = 1024;       // Sinks.Many 背压缓冲

    public String getServerHost() { return serverHost; }
    public void setServerHost(String serverHost) { this.serverHost = serverHost; }

    public int getServerPort() { return serverPort; }
    public void setServerPort(int serverPort) { this.serverPort = serverPort; }

    public String getClientId() { return clientId; }
    public void setClientId(String clientId) { this.clientId = clientId; }

    public int getKeepAliveSeconds() { return keepAliveSeconds; }
    public void setKeepAliveSeconds(int keepAliveSeconds) { this.keepAliveSeconds = keepAliveSeconds; }

    public MqttVersion getVersion() { return version; }
    public void setVersion(MqttVersion version) { this.version = version; }

    public int getSocketConnectTimeoutMs() { return socketConnectTimeoutMs; }
    public void setSocketConnectTimeoutMs(int ms) { this.socketConnectTimeoutMs = ms; }

    public int getMqttConnectTimeoutMs() { return mqttConnectTimeoutMs; }
    public void setMqttConnectTimeoutMs(int ms) { this.mqttConnectTimeoutMs = ms; }

    public TransportType getTransportType() { return transportType; }
    public void setTransportType(TransportType transportType) { this.transportType = transportType; }

    public MqttSslConfig getSslConfig() { return sslConfig; }
    public void setSslConfig(MqttSslConfig sslConfig) { this.sslConfig = sslConfig; }

    public MqttWebSocketConfig getWebSocketConfig() { return webSocketConfig; }
    public void setWebSocketConfig(MqttWebSocketConfig webSocketConfig) { this.webSocketConfig = webSocketConfig; }

    public int getNettyThreads() { return nettyThreads; }
    public void setNettyThreads(int nettyThreads) { this.nettyThreads = nettyThreads; }

    public LoopResources getLoopResources() { return loopResources; }
    public void setLoopResources(LoopResources loopResources) { this.loopResources = loopResources; }

    public boolean isCleanSession() { return cleanSession; }
    public void setCleanSession(boolean cleanSession) { this.cleanSession = cleanSession; }

    public long getSessionExpiryInterval() { return sessionExpiryInterval; }
    public void setSessionExpiryInterval(long sessionExpiryInterval) { this.sessionExpiryInterval = sessionExpiryInterval; }

    public int getReceiveMaximum() { return receiveMaximum; }
    public void setReceiveMaximum(int receiveMaximum) { this.receiveMaximum = receiveMaximum; }

    public String getUsername() { return username; }
    public void setUsername(String username) { this.username = username; }

    public byte[] getPassword() { return password; }
    public void setPassword(byte[] password) { this.password = password; }

    public MqttPublish getWillPublish() { return willPublish; }
    public void setWillPublish(MqttPublish willPublish) { this.willPublish = willPublish; }

    public boolean isAutomaticReconnect() { return automaticReconnect; }
    public void setAutomaticReconnect(boolean automaticReconnect) { this.automaticReconnect = automaticReconnect; }

    public long getReconnectInitialDelayMs() { return reconnectInitialDelayMs; }
    public void setReconnectInitialDelayMs(long ms) { this.reconnectInitialDelayMs = ms; }

    public long getReconnectMaxDelayMs() { return reconnectMaxDelayMs; }
    public void setReconnectMaxDelayMs(long ms) { this.reconnectMaxDelayMs = ms; }

    public int getMaxReconnectAttempts() { return maxReconnectAttempts; }
    public void setMaxReconnectAttempts(int maxReconnectAttempts) { this.maxReconnectAttempts = maxReconnectAttempts; }

    public int getMessageBufferMaxSize() { return messageBufferMaxSize; }
    public void setMessageBufferMaxSize(int messageBufferMaxSize) { this.messageBufferMaxSize = messageBufferMaxSize; }

    public long getMessageBufferMaxBytes() { return messageBufferMaxBytes; }
    public void setMessageBufferMaxBytes(long messageBufferMaxBytes) { this.messageBufferMaxBytes = messageBufferMaxBytes; }

    public boolean isClearBufferOnDisconnect() { return clearBufferOnDisconnect; }
    public void setClearBufferOnDisconnect(boolean clearBufferOnDisconnect) { this.clearBufferOnDisconnect = clearBufferOnDisconnect; }

    public int getMaxInflightMessages() { return maxInflightMessages; }
    public void setMaxInflightMessages(int maxInflightMessages) { this.maxInflightMessages = maxInflightMessages; }

    public int getInboxBufferSize() { return inboxBufferSize; }
    public void setInboxBufferSize(int inboxBufferSize) { this.inboxBufferSize = inboxBufferSize; }
}
