package plus.jmqx.client.mqtt.message;

import java.util.List;

/**
 * 版本无关的 UNSUBSCRIBE。
 *
 * @author maxid
 */
public interface MqttUnsubscribe {

    List<String> getTopicFilters();

    int getPacketId();
}
