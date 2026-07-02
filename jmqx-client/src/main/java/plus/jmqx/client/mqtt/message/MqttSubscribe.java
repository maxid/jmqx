package plus.jmqx.client.mqtt.message;

import java.util.List;

/**
 * 版本无关的 SUBSCRIBE。
 *
 * @author maxid
 * @since 1.4.14
 */
public interface MqttSubscribe {

    /**
     * 获取主题过滤器列表
     *
     * @return 主题过滤器列表
     */
    List<? extends MqttTopicFilter> getTopicFilters();

    /**
     * 获取 SUBSCRIBE 报文标识符
     *
     * @return SUBSCRIBE 报文标识符
     */
    int getPacketId();

}
