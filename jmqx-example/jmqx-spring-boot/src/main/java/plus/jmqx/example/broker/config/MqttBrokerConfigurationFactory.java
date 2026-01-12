package plus.jmqx.example.broker.config;

import cn.hutool.core.util.StrUtil;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.AbstractFactoryBean;
import org.springframework.stereotype.Component;
import plus.jmqx.broker.mqtt.MqttConfiguration;

import java.util.Map;
import java.util.Objects;

/**
 * MQTT服务配置
 *
 * @author maxid
 * @since 2025/4/26 10:52
 */
@Data
@Component
@EqualsAndHashCode(callSuper = true)
public class MqttBrokerConfigurationFactory
        extends AbstractFactoryBean<MqttConfiguration> implements FactoryBean<MqttConfiguration> {

    @Value(value = "${jmqx.tcp.port:1883}")
    private Integer port;

    @Value(value = "${jmqx.tcp.secure-port:8883}")
    private Integer securePort;

    @Value(value = "${jmqx.tcp.websocket-port:1884}")
    private Integer websocketPort;

    @Value(value = "${jmqx.tcp.websocket-secure-port:8884}")
    private Integer websocketSecurePort;

    @Value(value = "${jmqx.tcp.websocket-path:/mqtt}")
    private String websocketPath;

    @Value(value = "${jmqx.tcp.wiretap:false}")
    private Boolean wiretap;

    @Value(value = "${jmqx.tcp.boss-thread-size}")
    private Integer bossThreadSize;

    @Value(value = "${jmqx.tcp.work-thread-size}")
    private Integer workThreadSize;

    @Value(value = "${jmqx.tcp.business-thread-size}")
    private Integer businessThreadSize;

    @Value(value = "${jmqx.tcp.business-queue-size:100000}")
    private Integer businessQueueSize;

    @Value(value = "${jmqx.tcp.message-max-size:4194304}")
    private Integer messageMaxSize;

    @Value(value = "${jmqx.tcp.low-water-mark:32768}")
    private Integer lowWaterMark;

    @Value(value = "${jmqx.tcp.high-water-mark:65536}")
    private Integer highWaterMark;

    @Value(value = "${jmqx.tcp.global-read-write-size}")
    private String globalReadWriteSize;

    @Value(value = "${jmqx.tcp.channel-read-write-size}")
    private String channelReadWriteSize;

    @Value(value = "#{${jmqx.tcp.options:{}}}")
    private Map<String, Object> options;

    @Value(value = "#{${jmqx.tcp.child-options:{}}}")
    private Map<String, Object> childOptions;

    @Value(value = "${jmqx.ssl.enable:false}")
    private Boolean sslEnabled;

    @Value(value = "${jmqx.ssl.mode:classpath}")
    private String sslMode;

    @Value(value = "${jmqx.ssl.key}")
    private String sslKey;

    @Value(value = "${jmqx.ssl.crt}")
    private String sslCrt;

    @Value(value = "${jmqx.ssl.ca}")
    private String sslCa;

    @Value(value = "${jmqx.cluster.enable:false}")
    private Boolean clusterEnabled;

    @Value(value = "${jmqx.cluster.namespace:jmqx}")
    private String clusterNamespace;

    @Value(value = "${jmqx.cluster.url:}")
    private String clusterUrl;

    @Value(value = "${jmqx.cluster.port:7771}")
    private Integer clusterPort;

    @Value(value = "${jmqx.cluster.node:node-1}")
    private String clusterNode;

    @Override
    public Class<?> getObjectType() {
        return MqttConfiguration.class;
    }

    @Override
    protected MqttConfiguration createInstance() throws Exception {
        MqttConfiguration config = new MqttConfiguration();
        config.setPort(port);
        config.setSecurePort(securePort);
        config.setWebsocketPort(websocketPort);
        config.setWebsocketSecurePort(websocketSecurePort);
        config.setWebsocketPath(websocketPath);
        config.setWiretap(wiretap);
        if (bossThreadSize != null && bossThreadSize > 0) {
            config.setBossThreadSize(bossThreadSize);
        }
        if (workThreadSize != null && workThreadSize > 0) {
            config.setWorkThreadSize(workThreadSize);
        }
        if (businessThreadSize != null && businessThreadSize > 0) {
            config.setBusinessThreadSize(businessThreadSize);
        }
        if (businessQueueSize != null && businessQueueSize > 0) {
            config.setBusinessQueueSize(businessQueueSize);
        }
        config.setMessageMaxSize(messageMaxSize);
        config.setLowWaterMark(lowWaterMark);
        config.setHighWaterMark(highWaterMark);
        if (globalReadWriteSize != null) {
            config.setGlobalReadWriteSize(globalReadWriteSize);
        }
        if (channelReadWriteSize != null) {
            config.setChannelReadWriteSize(channelReadWriteSize);
        }
        if (Boolean.TRUE.equals(sslEnabled)) {
            config.setSslEnable(sslEnabled);
            if (StrUtil.isAllNotEmpty(sslKey, sslCrt, sslCa)) {
                if ("classpath".equalsIgnoreCase(sslMode)) {
                    config.setSslKey(Objects.requireNonNull(this.getClass().getResource(sslKey)).getPath());
                    config.setSslCrt(Objects.requireNonNull(this.getClass().getResource(sslCrt)).getPath());
                    config.setSslCa(Objects.requireNonNull(this.getClass().getResource(sslCa)).getPath());
                } else {
                    config.setSslKey(sslKey);
                    config.setSslCrt(sslCrt);
                    config.setSslCa(sslCa);
                }
            }
        }
        config.getClusterConfig().setEnabled(clusterEnabled);
        if (clusterEnabled) {
            config.getClusterConfig().setNamespace(clusterNamespace);
            config.getClusterConfig().setUrl(clusterUrl);
            config.getClusterConfig().setPort(clusterPort);
            config.getClusterConfig().setNode(clusterNode);
        }
        return config;
    }
}
