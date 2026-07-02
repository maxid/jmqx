package plus.jmqx.client.mqtt.v3.message;

import lombok.Builder;
import lombok.Value;
import plus.jmqx.client.mqtt.message.MqttUnsubscribe;

import java.util.List;

/**
 * MQTT 3.1.1 UNSUBSCRIBE（取消订阅）报文。
 *
 * <p>包含要取消订阅的主题过滤器列表及报文标识符。
 *
 * @author maxid
 */
@Value
@Builder(toBuilder = true)
public class Mqtt3Unsubscribe implements MqttUnsubscribe {

    /**
     * 要取消订阅的主题过滤器列表
     */
    List<String> topicFilters;
    /**
     * 报文标识符
     */
    int          packetId;

}
