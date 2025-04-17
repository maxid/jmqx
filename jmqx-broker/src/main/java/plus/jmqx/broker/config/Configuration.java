package plus.jmqx.broker.config;

import plus.jmqx.broker.mqtt.MqttConfiguration;

import java.util.Map;

/**
 * 配置接口
 *
 * @author maxid
 * @since 2025/4/8 17:57
 */
public interface Configuration {
    /**
     * Netty Boss 线程数
     *
     * @return {@link Integer} Netty Boss 线程数
     */
    Integer getBossThreadSize();

    /**
     * Netty Work 线程数
     *
     * @return {@link Integer} Netty Work 线程数
     */
    Integer getWorkThreadSize();

    /**
     * Netty 业务线程数
     *
     * @return {@link Integer} Netty 业务线程数
     */
    Integer getBusinessThreadSize();

    /**
     * Netty 工作队列数
     *
     * @return {@link Integer} Netty 工作队列数
     */
    Integer getBusinessQueueSize();

    /**
     * 消息最大限制值
     *
     * @return {@link Integer} 消息最大限制值
     */
    Integer getMessageMaxSize();

    /**
     * 全局读写限制
     *
     * @return {@link Integer} 全局读写限制
     */
    String getGlobalReadWriteSize();

    /**
     * 单个 Channel 读写限制
     *
     * @return {@link Integer} 单个 Channel 读写限制
     */
    String getChannelReadWriteSize();

    /**
     * Netty 低水位
     *
     * @return {@link Integer} Netty 低水位
     */
    Integer getLowWaterMark();

    /**
     * Netty 高水位
     *
     * @return {@link Integer} Netty 高水位
     */
    Integer getHighWaterMark();

    /**
     * 是否开启 Netty Tcp 二进制日志
     *
     * @return {@link Boolean} 是否开启 Netty Tcp 二进制日志
     */
    Boolean getWiretap();

    /**
     * Mqtt 端口
     *
     * @return {@link Integer} Mqtt 端口
     */
    Integer getPort();

    /**
     * Mqtts 端口
     *
     * @return {@link Integer} Mqtts 端口
     */
    Integer getSecurePort();

    /**
     * Mqtt Ws 端口
     *
     * @return {@link Integer} Mqtt Ws 端口
     */
    Integer getWebsocketPort();

    /**
     * Mqtt Wss 端口
     *
     * @return {@link Integer} Mqtt Wss 端口
     */
    Integer getWebsocketSecurePort();

    /**
     * Mqtt Ws 地址
     *
     * @return {@link String} Mqtt Ws 地址
     */
    String getWebsocketPath();

    /**
     * Netty Option 配置
     *
     * @return {@link Map} Netty Option 配置
     */
    Map<String, Object> getOptions();

    /**
     * Netty Child Option 配置
     *
     * @return {@link Map} Netty Child Option 配置
     */
    Map<String, Object> getChildOptions();

    /**
     * 是否启用 SSL
     *
     * @return {@link Boolean}
     */
    Boolean getSslEnable();

    /**
     * CA 证书路径
     *
     * @return {@link String} CA 证书路径
     */
    String getSslCa();

    /**
     * SSL 证书路径
     *
     * @return {@link String} SSL 证书路径
     */
    String getSslCrt();

    /**
     * SSL KEY 路径
     *
     * @return {@link String} SSL KEY 路径
     */
    String getSslKey();

    /**
     * 集群配置
     *
     * @return {@link MqttConfiguration.ClusterConfig} 集群配置
     */
    MqttConfiguration.ClusterConfig getClusterConfig();
}
