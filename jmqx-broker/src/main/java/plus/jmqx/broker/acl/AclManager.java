package plus.jmqx.broker.acl;

import plus.jmqx.broker.mqtt.channel.MqttSession;
import plus.jmqx.broker.spi.DynamicLoader;

/**
 * 主题访问控制列表管理
 *
 * @author maxid
 * @since 2025/4/8 17:39
 */
public interface AclManager {

    /**
     * 用户自定义实例
     */
    AclManager INSTANCE = DynamicLoader.findFirst(AclManager.class).orElse(null);

    /**
     * 主题访问控制权限校验
     *
     * @param session 会话
     * @param topic   主题名称
     * @param action  校验权限
     * @return 是否具备指定权限
     */
    boolean check(MqttSession session, String topic, AclAction action);

    /**
     * 是否必须卸载到独立线程池执行 {@link #check}。
     * <p>
     * 默认 {@code true}（保守：自定义实现可能含 Feign/DB 等阻塞调用）。
     * 内存规则、恒允许等非阻塞实现应返回 {@code false}，以便在
     * {@code jmqx-publish}/{@code jmqx-control} 上内联校验，避免无谓的 Offload + 回流切换。
     *
     * @return {@code true} 时走 AclExecutor Offload；{@code false} 时调用方可同步内联
     */
    default boolean requiresOffload() {
        return true;
    }

}
