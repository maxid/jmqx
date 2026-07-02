package plus.jmqx.client.mqtt.message;

import java.util.List;

/**
 * 版本无关的 SUBACK。
 *
 * @author maxid
 */
public interface MqttSubAck {

    List<QoS> getGrantedQos();

    int getPacketId();
}
