package plus.jmqx.client.mqtt.internal.transport;

import lombok.Builder;
import lombok.Data;

/**
 * SSL/TLS 配置。
 *
 * @author maxid
 */
@Data
@Builder
public class MqttSslConfig {

    private String   trustStorePath;
    private String   trustStorePassword;
    private String   keyStorePath;
    private String   keyStorePassword;
    private String[] cipherSuites;
    private String[] protocols;
    private int      handshakeTimeoutMs = 10_000;

}
