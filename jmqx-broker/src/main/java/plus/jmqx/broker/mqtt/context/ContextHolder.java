package plus.jmqx.broker.mqtt.context;

/**
 * 全局上下文工具
 *
 * @author maxid
 * @since 2025/4/10 14:30
 */
public class ContextHolder {
    private static ReceiveContext<?> context;

    public static ReceiveContext<?> getContext() {
        return ContextHolder.context;
    }

    public static void setContext(ReceiveContext<?> context) {
        ContextHolder.context = context;
    }
}
