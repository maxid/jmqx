package plus.jmqx.broker.mqtt.context;

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
public class ContextHolder {
    /**
     * 上下文
     */
    private static ReceiveContext<?>  context;
    /**
     * 主题访问控制列表管理
     */
    private static AclManager         aclManager;
    /**
     * 设备连接鉴权管理
     */
    private static AuthManager        authManager;
    /**
     * MQTT 生命周开放接口
     */
    private static PlatformDispatcher platformDispatcher;
    /**
     *
     */
    private static Scheduler          dispatchScheduler;

    /**
     * 获取上下文
     *
     * @return 上下文
     */
    public static ReceiveContext<?> getContext() {
        return ContextHolder.context;
    }

    /**
     * 设置上下文
     *
     * @param context 上下文
     */
    public static void setContext(ReceiveContext<?> context) {
        ContextHolder.context = context;
    }

    /**
     * 获取主题访问控制列表管理
     *
     * @return 主题访问控制列表管理
     */
    public static AclManager getAclManager() {
        return ContextHolder.aclManager;
    }

    /**
     * 设置主题访问控制列表管理
     *
     * @param aclManager 主题访问控制列表管理
     */
    public static void setAclManager(AclManager aclManager) {
        ContextHolder.aclManager = aclManager;
    }

    /**
     * 获取设备连接鉴权管理
     *
     * @return 设备连接鉴权管理
     */
    public static AuthManager getAuthManager() {
        return ContextHolder.authManager;
    }

    /**
     * 设置设备连接鉴权管理
     *
     * @param authManager 设备连接鉴权管理
     */
    public static void setAuthManager(AuthManager authManager) {
        ContextHolder.authManager = authManager;
    }

    /**
     * 获取 MQTT 生命周开放接口
     *
     * @return MQTT 生命周开放接口
     */
    public static PlatformDispatcher getPlatformDispatcher() {
        return ContextHolder.platformDispatcher;
    }

    /**
     * 设置 MQTT 生命周开放接口
     *
     * @param platformDispatcher MQTT 生命周开放接口
     */
    public static void setPlatformDispatcher(PlatformDispatcher platformDispatcher) {
        ContextHolder.platformDispatcher = platformDispatcher;
    }

    public static Scheduler getDispatchScheduler() {
        return ContextHolder.dispatchScheduler;
    }

    public static void setDispatchScheduler(Scheduler dispatchScheduler) {
        ContextHolder.dispatchScheduler = dispatchScheduler;
    }

}
