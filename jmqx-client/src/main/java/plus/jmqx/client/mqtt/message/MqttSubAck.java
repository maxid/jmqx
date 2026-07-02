package plus.jmqx.client.mqtt.message;

import java.util.List;

/**
 * 版本无关的 SUBACK。
 *
 * @author maxid
 * @since 1.4.14
 */
public interface MqttSubAck {

    /**
     * 获取 broker 授予的 QoS 等级列表
     *
     * @return broker 授予的 QoS 等级列表
     */
    List<QoS> getGrantedQos();

    /**
     * 获取 SUBACK 报文标识符
     *
     * @return SUBACK 报文标识符
     */
    int getPacketId();

}
