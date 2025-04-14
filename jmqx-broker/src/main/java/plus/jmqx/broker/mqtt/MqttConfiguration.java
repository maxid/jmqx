package plus.jmqx.broker.mqtt;

import lombok.Data;
import plus.jmqx.broker.config.Configuration;

import java.util.Map;

/**
 * MQTT 配置
 *
 * @author maxid
 * @since 2025/4/9 11:45
 */
@Data
public class MqttConfiguration implements Configuration {
    private Integer             bossThreadSize      = Runtime.getRuntime().availableProcessors();
    private Integer             workThreadSize      = Runtime.getRuntime().availableProcessors() * 2;
    private Integer             businessThreadSize  = Runtime.getRuntime().availableProcessors();
    private Integer             businessQueueSize   = 100000;
    private Integer             messageMaxSize      = 4194304;
    private String              globalReadWriteSize;
    private String              channelReadWriteSize;
    private Integer             lowWaterMark;
    private Integer             highWaterMark;
    private Boolean             wiretap             = false;
    private Integer             port                = 1883;
    private Integer             securePort          = 1884;
    private Integer             websocketPort       = 8883;
    private Integer             websocketSecurePort = 8884;
    private Map<String, Object> options;
    private Map<String, Object> childOptions;
    private Boolean             sslEnable           = false;
    private String              sslCa;
    private String              sslCrt;
    private String              sslKey;
}
