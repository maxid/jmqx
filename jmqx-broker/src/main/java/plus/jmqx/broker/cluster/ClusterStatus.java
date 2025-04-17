package plus.jmqx.broker.cluster;

/**
 * 集群节点状态
 *
 * @author maxid
 * @since 2025/4/17 09:50
 */
public enum ClusterStatus {
    /**
     * 添加
     */
    ADDED,
    /**
     * 移除
     */
    REMOVED,
    /**
     * 离开
     */
    LEAVING,
    /**
     * 更新
     */
    UPDATED
}
