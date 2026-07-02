package plus.jmqx.client.mqtt.v5.message;

import lombok.Builder;
import lombok.Value;
import plus.jmqx.client.mqtt.message.MqttSubscribe;

import java.util.List;

/**
 * MQTT 5.0 SUBSCRIBE。
 *
 * @author maxid
 */
@Value
@Builder(toBuilder = true)
public class Mqtt5Subscribe implements MqttSubscribe {

    /** 主题过滤器列表 */
    List<Mqtt5TopicFilter> topicFilters;
    /** 报文标识符 */
    int                    packetId;

}
