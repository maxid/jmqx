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

    /**
     * 信任库路径
     */
    private String   trustStorePath;
    /**
     * 信任库密码
     */
    private String   trustStorePassword;
    /**
     * 密钥库路径
     */
    private String   keyStorePath;
    /**
     * 密钥库密码
     */
    private String   keyStorePassword;
    /**
     * 加密套件
     */
    private String[] cipherSuites;
    /**
     * 协议版本
     */
    private String[] protocols;
    /**
     * 握手超时时间（毫秒），默认 10000
     */
    private int      handshakeTimeoutMs = 10_000;
    /**
     * 信任所有服务端证书（仅用于测试环境，生产环境勿用）
     */
    private boolean  insecureTrustAll;

}
