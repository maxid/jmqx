package plus.jmqx.broker.mqtt.registry;

import java.util.Map;

/**
 * 注入环境变量
 *
 * @author maxid
 * @since 2025/4/16 14:13
 */
public interface Startup {
    /**
     * 注入环境变量
     *
     * @param env 注入不同配置
     */
    default void startup(Map<Object, Object> env) {

    }
}
