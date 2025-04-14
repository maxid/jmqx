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
    private Integer             bossThreadSize       = Runtime.getRuntime().availableProcessors();
    private Integer             workThreadSize       = Runtime.getRuntime().availableProcessors() * 2;
    private Integer             businessThreadSize   = Runtime.getRuntime().availableProcessors();
    private Integer             businessQueueSize    = 100000;
    private Integer             messageMaxSize       = 4194304;
    private String              globalReadWriteSize  = "10000000,100000000"; // 全局读写大小限制
    private String              channelReadWriteSize = "10000000,100000000"; // 单个channel读写大小限制
    private Integer             lowWaterMark         = 4000000; // 不建议配置 默认 32768
    private Integer             highWaterMark        = 80000000; // 不建议配置 默认 65536
    private Boolean             wiretap              = true; // 二进制日志 前提是 smqtt.logLevel = DEBUG
    private Integer             port                 = 1883; // IANA MQTT 保留端口
    private Integer             securePort           = 8883; // IANA MQTTS 保留端口
    private Integer             websocketPort        = 1884; // 自定义 MQTT WS 服务端口
    private Integer             websocketSecurePort  = 8884; // 自定义 MQTT WSS 服务端口
    private String              websocketPath        = "/mqtt";
    private Map<String, Object> options;
    private Map<String, Object> childOptions;
    private Boolean             sslEnable            = false;
    private String              sslCa;
    private String              sslCrt;
    private String              sslKey;
}
