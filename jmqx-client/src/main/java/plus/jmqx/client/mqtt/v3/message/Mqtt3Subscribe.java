package plus.jmqx.client.mqtt.v3.message;

import lombok.Builder;
import lombok.Value;
import plus.jmqx.client.mqtt.message.MqttSubscribe;

import java.util.List;

/**
 * MQTT 3.1.1 SUBSCRIBE（订阅）报文。
 *
 * <p>包含一组主题过滤器及其对应的报文标识符。
 *
 * @author maxid
 */
@Value
@Builder(toBuilder = true)
public class Mqtt3Subscribe implements MqttSubscribe {

    /** 主题过滤器列表 */
    List<Mqtt3TopicFilter> topicFilters;
    /** 报文标识符 */
    int                    packetId;

}
