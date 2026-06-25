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
        return get(namespace, null);
    }

    /**
     * 获取上下文（命名空间 + 节点）
     *
     * @param namespace 命名空间
     * @param node      节点名称（允许 null 或空，此时退化为仅 namespace 为键）
     * @return 上下文
     */
    public static ContextHolder get(String namespace, String node) {
        String key = key(namespace, node);
        if (!WRAPPER.containsKey(key)) {
            WRAPPER.put(key, ContextHolder.builder().build());
        }
        return WRAPPER.get(key);
    }

    /**
     * 检查命名空间冲突
     *
     * @param namespace 命名空间
     * @throws Exception 命名空间冲突异常
     */
    public static void checkNamespace(String namespace) throws Exception {
        checkNamespace(namespace, null);
    }

    /**
     * 检查命名空间冲突（命名空间 + 节点）
     *
     * @param namespace 命名空间
     * @param node      节点名称
     * @throws Exception 命名空间冲突异常
     */
    public static void checkNamespace(String namespace, String node) throws Exception {
        String key = key(namespace, node);
        if (WRAPPER.containsKey(key)) {
            throw new Exception(new IllegalStateException(String.format("namespace %s node %s is exist", namespace, node)));
        }
    }

    /**
     * 生成组合键
     *
     * @param namespace 命名空间
     * @param node      节点名称
     * @return 组合键
     */
    private static String key(String namespace, String node) {
        return node != null && !node.isEmpty() ? namespace + ":" + node : namespace;
    }

}
