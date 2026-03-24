package plus.jmqx.broker.mqtt.retry;

/**
 * MQTT ACK 管理
 *
 * @author maxid
 * @since 2025/4/11 09:12
 */
public interface AckManager {

    /**
     * 添加 ACK。
     *
     * @param ack ACK 实例
     */
    void addAck(Ack ack);

    /**
     * 获取 ACK。
     *
     * @param id ACK ID
     * @return ACK 实例
     */
    Ack getAck(Long id);

    /**
     * 删除 ACK。
     *
     * @param id ACK ID
     */
    void deleteAck(Long id);

}
