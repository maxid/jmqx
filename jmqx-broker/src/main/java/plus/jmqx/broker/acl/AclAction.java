package plus.jmqx.broker.acl;

/**
 * 主题访问控制权限
 *
 * @author maxid
 * @since 2025/4/18 11:41
 */
public enum AclAction {
    /**
     * 订阅权限
     */
    SUBSCRIBE,
    /**
     * 发布权限
     */
    PUBLISH,
    /**
     * 订阅与发布权限
     */
    ALL
}
