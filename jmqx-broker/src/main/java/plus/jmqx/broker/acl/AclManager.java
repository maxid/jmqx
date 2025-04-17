package plus.jmqx.broker.acl;

import plus.jmqx.broker.spi.DynamicLoader;

/**
 * 主题访问控制列表管理
 *
 * @author maxid
 * @since 2025/4/8 17:39
 */
public interface AclManager {
    AclManager INSTANCE = DynamicLoader.findFirst(AclManager.class).orElse(null);


}
