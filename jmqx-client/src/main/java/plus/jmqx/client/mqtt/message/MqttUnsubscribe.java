package plus.jmqx.client.mqtt.message;

import java.util.List;

/**
 * 版本无关的 UNSUBSCRIBE。
 *
 * @author maxid
 * @since 1.4.14
 */
public interface MqttUnsubscribe {

    /**
     * 获取待取消订阅的主题过滤器列表
     *
     * @return 待取消订阅的主题过滤器列表
     */
    List<String> getTopicFilters();

    /**
     * 获取 UNSUBSCRIBE 报文标识符
     *
     * @return UNSUBSCRIBE 报文标识符
     */
    int getPacketId();

}
