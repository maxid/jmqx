package plus.jmqx.broker.mqtt.message.interceptor;

import lombok.AllArgsConstructor;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

/**
 * 拦截处理
 *
 * @author maxid
 * @since 2025/4/16 17:21
 */
@AllArgsConstructor
public class InterceptorHandler implements InvocationHandler {

    private final Interceptor interceptor;

    private final Object target;

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        Intercept intercept = method.getAnnotation(Intercept.class);
        if (intercept == null) {
            return method.invoke(target, args);
        } else {
            return interceptor.intercept(new Invocation(method, target, args));
        }
    }
}
