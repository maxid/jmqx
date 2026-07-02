package plus.jmqx.client.mqtt.message;

import java.util.List;

/**
 * 版本无关的 SUBSCRIBE。
 *
 * @author maxid
 */
public interface MqttSubscribe {

    List<? extends MqttTopicFilter> getTopicFilters();

    int getPacketId();
}
