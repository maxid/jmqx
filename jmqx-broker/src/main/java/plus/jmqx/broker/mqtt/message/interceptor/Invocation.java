package plus.jmqx.broker.mqtt.message.interceptor;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Method;

/**
 * 调用
 *
 * @author maxid
 * @since 2025/4/16 17:15
 */
@Slf4j
@Getter
@AllArgsConstructor
public class Invocation {
    /**
     * 方法
     */
    private final Method   method;
    /**
     * 目标对接
     */
    private final Object   target;
    /**
     * 参数
     */
    private final Object[] args;

    /**
     * 继续执行
     *
     * @return 返回结果
     */
    public Object proceed() {
        try {
            return method.invoke(target, args);
        } catch (Exception e) {
            log.error("invocation {} proceed error", this, e);
            return null;
        }
    }
}
