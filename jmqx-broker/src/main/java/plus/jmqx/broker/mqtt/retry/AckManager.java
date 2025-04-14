package plus.jmqx.broker.mqtt.retry;

/**
 * MQTT QoS ACK 管理
 *
 * @author maxid
 * @since 2025/4/11 09:12
 */
public interface AckManager {
    void addAck(Ack ack);

    Ack getAck(Long id);

    void deleteAck(Long id);
}
