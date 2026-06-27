package plus.jmqx.broker.metrics;

import plus.jmqx.broker.metrics.impl.NoopMetricsManager;
import plus.jmqx.broker.spi.DynamicLoader;

/**
 * 指标管理器持有者
 * <p>
 * 通过 SPI 加载 MetricsManager 实现，未找到时使用 NoopMetricsManager。
 * 全局唯一实例。
 * </p>
 *
 * @author maxid
 * @since 2025/4/16
 */
public class MetricsManagerHolder {

    private static volatile MetricsManager instance;

    private MetricsManagerHolder() {
    }

    /**
     * 获取指标管理器实例
     *
     * @return MetricsManager 实例
     */
    public static MetricsManager get() {
        if (instance == null) {
            synchronized (MetricsManagerHolder.class) {
                if (instance == null) {
                    instance = DynamicLoader.findFirst(MetricsManager.class)
                            .orElseGet(NoopMetricsManager::new);
                }
            }
        }
        return instance;
    }

    /**
     * 重置实例（主要用于测试）
     */
    static void reset() {
        instance = null;
    }

}
