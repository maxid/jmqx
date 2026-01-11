package plus.jmqx.broker.mqtt.context;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 支持多命名空间的上下文持有器
 *
 * @author maxid
 * @since 2026/1/11 15:38
 */
public class NamespaceContextHolder {

    private static final Map<String, ContextHolder> WRAPPER = new ConcurrentHashMap<>();

    /**
     * 获取上下文
     *
     * @param namespace 命名空间
     * @return 上下文
     */
    public static ContextHolder get(String namespace) {
        if (!WRAPPER.containsKey(namespace)) {
            WRAPPER.put(namespace, ContextHolder.builder().build());
        }
        return WRAPPER.get(namespace);
    }

    /**
     * 检查命名空间冲突
     *
     * @param namespace 命名空间
     * @throws Exception 命名空间冲突异常
     */
    public static void checkNamespace(String namespace) throws Exception {
        if (WRAPPER.containsKey(namespace)) {
            throw new Exception(new IllegalStateException(String.format("namespace %s is exist", namespace)));
        }
    }

}
