package plus.jmqx.broker.mqtt.message.interceptor;

import plus.jmqx.broker.mqtt.message.MessageDispatcher;

import java.lang.reflect.Proxy;

/**
 * 拦截器接口
 * <p>
 * <b>线程契约：</b>{@link #intercept(Invocation)} 在消息分发调用线程上同步执行
 * （可能是 Netty Event Loop、{@code jmqx-publish}/{@code jmqx-control} 等）。
 * 实现必须非阻塞；禁止 JDBC、同步 HTTP、Feign、{@code Thread.sleep} 等。
 * 若需阻塞 I/O，请自行卸载到专用线程池后再返回。
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
