package plus.jmqx.broker.metrics.impl;

import plus.jmqx.broker.metrics.MetricsManager;

/**
 * 无操作指标管理器（默认实现）
 * <p>
 * 当无 SPI 实现注册时使用，所有方法均为空操作。
 * </p>
 *
 * @author maxid
 * @since 2025/4/16
 */
public class NoopMetricsManager implements MetricsManager {

    public NoopMetricsManager() {
    }

}
