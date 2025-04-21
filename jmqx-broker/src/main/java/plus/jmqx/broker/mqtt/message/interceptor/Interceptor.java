package plus.jmqx.broker.mqtt.message.interceptor;

import plus.jmqx.broker.mqtt.message.MessageDispatcher;

import java.lang.reflect.Proxy;

/**
 * 拦截器接口
 *
 * @author maxid
 * @since 2025/4/16 17:14
 */
public interface Interceptor {
    /**
     * 拦截目标参数
     *
     * @param invocation {@link Invocation}
     * @return Object
     */
    Object intercept(Invocation invocation);

    /**
     * 代理
     *
     * @param adapter {{@link MessageDispatcher} 消息处理适配器
     * @return 代理类
     */
    default MessageDispatcher proxy(MessageDispatcher adapter) {
        return (MessageDispatcher) Proxy.newProxyInstance(adapter.getClass().getClassLoader(), new Class[]{MessageDispatcher.class}, new InterceptorHandler(this, adapter));
    }

    /**
     * 排序
     *
     * @return 排序
     */
    int sort();
}
