package plus.jmqx.broker.mqtt.context;

import lombok.Builder;
import lombok.Data;
import plus.jmqx.broker.acl.AclManager;
import plus.jmqx.broker.auth.AuthManager;
import plus.jmqx.broker.mqtt.message.dispatch.PlatformDispatcher;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * 全局上下文工具
 *
 * @author maxid
 * @since 2025/4/10 14:30
 */
@Data
@Builder
public final class ContextHolder {
    /**
     * 上下文
     */
    private ReceiveContext<?>  context;
    /**
     * 主题访问控制列表管理
     */
    private AclManager         aclManager;
    /**
     * 设备连接鉴权管理
     */
    private AuthManager        authManager;
    /**
     * MQTT 生命周开放接口
     */
    private PlatformDispatcher platformDispatcher;
    /**
     * 调度器
     */
    private Scheduler          dispatchScheduler;
}
