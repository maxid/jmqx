package plus.jmqx.broker.mqtt;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import plus.jmqx.broker.config.Configuration;
import plus.jmqx.broker.config.ConnectMode;

import java.util.Map;

/**
 * MQTT 配置
 *
 * @author maxid
 * @since 2025/4/9 11:45
 */
@Data
public class MqttConfiguration implements Configuration {
    /**
     * 默认命名空间
     */
    public static final String              DEFAULT_NAMESPACE    = "jmqx-broker";
    /**
     * Netty Boss 线程数
     */
    private             Integer             bossThreadSize       = Runtime.getRuntime().availableProcessors();
    /**
     * Netty Work 线程数
     */
    private             Integer             workThreadSize       = Runtime.getRuntime().availableProcessors() * 2;
    /**
     * Netty 业务线程数
     */
    private             Integer             businessThreadSize   = Runtime.getRuntime().availableProcessors() * 4;
    /**
     * Netty 工作队列数、
     */
    private             Integer             businessQueueSize    = 100000;
    /**
     * 消息最大限制值
     */
    private             Integer             messageMaxSize       = 4194304;
    /**
     * 全局读写大小限制
     */
    private             String              globalReadWriteSize  = "10000000,100000000";
    /**
     * 单个 channel 读写大小限制
     */
    private             String              channelReadWriteSize = "10000000,100000000";
    /**
     * Netty 低水位，不建议配置 默认 32768
     */
    private             Integer             lowWaterMark         = 4000000;
    /**
     * Netty 高水位，不建议配置 默认 65536
     */
    private             Integer             highWaterMark        = 80000000;
    /**
     * 是否开启 Netty Tcp 二进制日志，前提是 logLevel = DEBUG
     */
    private             Boolean             wiretap              = true;
    /**
     * Mqtt 端口，IANA MQTT 保留端口
     */
    private             Integer             port                 = 1883;
    /**
     * Mqtts 端口，IANA MQTTS 保留端口
     */
    private             Integer             securePort           = 8883;
    /**
     * Mqtt Ws 端口，自定义 MQTT WS 服务端口
     */
    private             Integer             websocketPort        = 1884;
    /**
     * Mqtt Wss 端口，自定义 MQTT WSS 服务端口
     */
    private             Integer             websocketSecurePort  = 8884;
    /**
     * Mqtt Ws 端点地址
     */
    private             String              websocketPath        = "/mqtt";
    /**
     * 连接模式
     */
    private             ConnectMode         connectMode          = ConnectMode.UNIQUE;
    /**
     * 指定时间窗内不踢出
     */
    private             Integer             notKickSeconds       = 30;
    /**
     * Netty Option 配置
     */
    private             Map<String, Object> options;
    /**
     * Netty Child Option 配置
     */
    private             Map<String, Object> childOptions;
    /**
     * 是否启用 SSL
     */
    private             Boolean             sslEnable            = false;
    /**
     * CA 证书路径
     */
    private             String              sslCa;
    /**
     * SSL 证书路径
     */
    private             String              sslCrt;
    /**
     * SSL KEY 路径
     */
    private             String              sslKey;
    /**
     * 集群配置
     */
    @JsonProperty("cluster")
    private             ClusterConfig       clusterConfig        = ClusterConfig.builder()
            .enabled(false)
            .namespace(DEFAULT_NAMESPACE)
            .build();

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ClusterConfig {
        /**
         * 开启集群
         */
        private boolean         enabled;
        /**
         * 集群url
         */
        private String          url;
        /**
         * 集群启动本地端口
         */
        private Integer         port;
        /**
         * 集群名称 需要唯一
         */
        private String          node;
        /**
         * 集群空间 需要一致才能通信
         */
        private String          namespace;
        /**
         * 集群额外配置（主要用于容器映射）
         */
        private ClusterExternal external;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ClusterExternal {
        /**
         * 本地曝光host
         */
        private String  host;
        /**
         * 本地曝光port
         */
        private Integer port;
    }
}
