package plus.jmqx.client.mqtt.stress;

import plus.jmqx.client.mqtt.MqttClientConfig;

/**
 * 压测传输层类型（{@code -Djmqx.client.stress.transport=}）。
 */
public enum ClientStressTransport {

    TCP(MqttClientConfig.TransportType.TCP, "tcp", 1883),
    TLS(MqttClientConfig.TransportType.TLS, "mqtts", 8883),
    WS(MqttClientConfig.TransportType.WS, "ws", 1884),
    WSS(MqttClientConfig.TransportType.WSS, "wss", 8884);

    private final MqttClientConfig.TransportType transportType;
    private final String                         label;
    private final int                            defaultPort;

    ClientStressTransport(MqttClientConfig.TransportType transportType, String label, int defaultPort) {
        this.transportType = transportType;
        this.label = label;
        this.defaultPort = defaultPort;
    }

    public MqttClientConfig.TransportType transportType() {
        return transportType;
    }

    public String label() {
        return label;
    }

    public int defaultPort() {
        return defaultPort;
    }

    public String stressPortProperty() {
        return switch (this) {
            case TCP -> "jmqx.client.stress.broker.port";
            case TLS -> "jmqx.client.stress.broker.securePort";
            case WS -> "jmqx.client.stress.broker.websocketPort";
            case WSS -> "jmqx.client.stress.broker.websocketSecurePort";
        };
    }

    public String itPortProperty() {
        return switch (this) {
            case TCP -> "jmqx.it.broker.port";
            case TLS -> "jmqx.it.broker.securePort";
            case WS -> "jmqx.it.broker.websocketPort";
            case WSS -> "jmqx.it.broker.websocketSecurePort";
        };
    }

    public static ClientStressTransport parse(String value) {
        if (value == null || value.isEmpty()) {
            return TCP;
        }
        String normalized = value.trim().toLowerCase();
        return switch (normalized) {
            case "tcp", "mqtt" -> TCP;
            case "tls", "mqtts", "ssl" -> TLS;
            case "ws", "websocket" -> WS;
            case "wss" -> WSS;
            default -> throw new IllegalArgumentException(
                    "unknown stress transport: " + value + " (expected tcp|mqtts|ws|wss)");
        };
    }

}
