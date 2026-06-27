package plus.jmqx.broker.metrics;

/**
 * 指标管理器 SPI
 * <p>
 * 提供可插拔的指标收集能力。SPI 实现可通过 META-INF/services 注册，
 * 系统启动时自动加载。若找不到实现，使用 NoopMetricsManager。
 * </p>
 *
 * @author maxid
 * @since 2025/4/16
 */
public interface MetricsManager {

    /**
     * 连接数 +1
     */
    default void incrementConnections() {
    }

    /**
     * 连接数 -1
     */
    default void decrementConnections() {
    }

    /**
     * 发布消息数 +1
     */
    default void incrementPublishedMessages() {
    }

    /**
     * 订阅事件数 +1
     */
    default void incrementSubscribeEvents() {
    }

    /**
     * 订阅事件数 -1
     */
    default void decrementSubscribeEvents() {
    }

    /**
     * 断开连接事件数 +1
     */
    default void incrementDisconnectEvents() {
    }

    /**
     * 丢弃消息记录
     *
     * @param reason 丢弃原因
     */
    default void recordDroppedMessage(String reason) {
    }

    /**
     * Sink 溢出记录
     *
     * @param sinkName Sink 名称
     */
    default void recordOverflow(String sinkName) {
    }

    /**
     * 记录队列深度
     *
     * @param queueName 队列名称
     * @param depth     当前深度
     */
    default void recordQueueDepth(String queueName, int depth) {
    }

    /**
     * 记录当前连接数
     *
     * @param count 连接数
     */
    default void recordConnections(int count) {
    }

}
