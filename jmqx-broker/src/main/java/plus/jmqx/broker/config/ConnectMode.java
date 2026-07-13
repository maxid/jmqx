package plus.jmqx.broker.config;

/**
 * 连接模式
 *
 * @author maxid
 * @since 2025/4/17 16:30
 */
public enum ConnectMode {

    /**
     * 唯一，保障旧设备连接安全，直接拒绝新设备连接 Broker
     */
    UNIQUE,
    /**
     * 踢出,
     */
    KICK

}
