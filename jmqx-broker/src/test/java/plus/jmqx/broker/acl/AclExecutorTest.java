package plus.jmqx.broker.acl;

import org.junit.jupiter.api.Test;
import plus.jmqx.broker.mqtt.channel.MqttSession;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * ACL 执行器单元测试
 *
 * @author maxid
 * @since 2026/7/23 23:30
 */
class AclExecutorTest {

    @Test
    void checkSupportsSyncAclManager() {
        AclManager aclManager = (session, topic, action) -> true;
        AclExecutor executor = new AclExecutor(aclManager, "mqtt", 1000, 8, 1000);
        Boolean passed = executor.check(null, "t/1", AclAction.SUBSCRIBE).join();
        assertEquals(Boolean.TRUE, passed);
    }

    @Test
    void checkReturnsFalseWhenAclException() {
        AclManager aclManager = (session, topic, action) -> {
            throw new IllegalStateException("acl error");
        };
        AclExecutor executor = new AclExecutor(aclManager, "mqtt", 1000, 8, 1000);
        Boolean passed = executor.check(null, "t/1", AclAction.PUBLISH).join();
        assertEquals(Boolean.FALSE, passed);
    }

    @Test
    void checkReturnsFalseWhenAclTimeout() {
        AclManager aclManager = (session, topic, action) -> {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return true;
        };
        AclExecutor executor = new AclExecutor(aclManager, "mqtt", 50, 8, 1000);
        Boolean passed = executor.check((MqttSession) null, "t/1", AclAction.SUBSCRIBE).join();
        assertNotEquals(Boolean.TRUE, passed);
    }

    @Test
    void defaultAclManagerSkipsOffload() {
        AclExecutor executor = new AclExecutor(new plus.jmqx.broker.acl.impl.DefaultAclManager(),
                "mqtt", 1000, 8, 1000);
        assertEquals(false, executor.requiresOffload());
        assertEquals(true, executor.checkInline(null, "t/1", AclAction.PUBLISH));
        assertEquals(Boolean.TRUE, executor.check(null, "t/1", AclAction.PUBLISH).join());
    }

    @Test
    void customAclManagerRequiresOffloadByDefault() {
        AclManager aclManager = (session, topic, action) -> true;
        AclExecutor executor = new AclExecutor(aclManager, "mqtt", 1000, 8, 1000);
        assertEquals(true, executor.requiresOffload());
    }

}
