package plus.jmqx.client.mqtt.v5.message;

import lombok.Builder;
import lombok.Value;
import plus.jmqx.client.mqtt.message.MqttUnsubscribe;

import java.util.List;

/**
 * MQTT 5.0 UNSUBSCRIBE。
 *
 * @author maxid
 */
@Value
@Builder(toBuilder = true)
public class Mqtt5Unsubscribe implements MqttUnsubscribe {

    /** 要取消订阅的主题过滤器列表 */
    List<String> topicFilters;
    /** 报文标识符 */
    int          packetId;

}
