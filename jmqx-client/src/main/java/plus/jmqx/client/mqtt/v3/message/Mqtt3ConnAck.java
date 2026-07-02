package plus.jmqx.client.mqtt.v3.message;

import lombok.Value;
import plus.jmqx.client.mqtt.message.MqttConnAck;

/**
 * MQTT 3.1.1 CONNACK（连接确认）报文。
 *
 * <p>包含 sessionPresent 标志和连接返回码 {@link Mqtt3ConnAckReturnCode}。
 *
 * @author maxid
 */
@Value
public class Mqtt3ConnAck implements MqttConnAck {

    /** 服务端是否已恢复之前的会话 */
    boolean                sessionPresent;
    /** 连接返回码 */
    Mqtt3ConnAckReturnCode returnCode;

    @Override
    public boolean isSessionPresent() {
        return sessionPresent;
    }

}
