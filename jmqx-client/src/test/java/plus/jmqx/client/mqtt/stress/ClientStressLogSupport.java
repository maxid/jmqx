package plus.jmqx.client.mqtt.stress;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import org.slf4j.LoggerFactory;

/**
 * jmqx-client 压测日志级别控制。
 *
 * <p>默认 {@code WARN}，不输出 reactor-netty 十六进制报文。可通过
 * {@code -Djmqx.client.stress.logLevel=INFO|DEBUG} 调整；压测结果日志（{@code plus.jmqx.client.mqtt.stress}）
 * 始终为 INFO。
 */
public final class ClientStressLogSupport {

    private ClientStressLogSupport() {
    }

    /**
     * 按系统属性配置压测日志级别。
     *
     * <p>属性 {@code jmqx.client.stress.logLevel}，默认 {@code WARN}。
     */
    public static void configure() {
        Level level = parseLevel(System.getProperty("jmqx.client.stress.logLevel", "WARN"));
        LoggerContext ctx = (LoggerContext) LoggerFactory.getILoggerFactory();
        setLevel(ctx.getLogger("root"), level);
        // 抑制 TCP/MQTT 十六进制报文与 pipeline 调试输出
        setLevel(ctx.getLogger("reactor.netty"), Level.WARN);
        setLevel(ctx.getLogger("io.netty"), Level.WARN);
        setLevel(ctx.getLogger("plus.jmqx.broker"), level);
        setLevel(ctx.getLogger("plus.jmqx.broker.mqtt.message.impl"), level);
        setLevel(ctx.getLogger("plus.jmqx.broker.mqtt.channel.MqttSession"), level);
        setLevel(ctx.getLogger("plus.jmqx.client"), level);
        setLevel(ctx.getLogger("plus.jmqx.client.mqtt.internal"), level);
        setLevel(ctx.getLogger("ack"), Level.WARN);
        // 压测吞吐/结果始终 INFO
        setLevel(ctx.getLogger("plus.jmqx.client.mqtt.stress"), Level.INFO);
    }

    private static Level parseLevel(String name) {
        try {
            return Level.valueOf(name.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            return Level.WARN;
        }
    }

    private static void setLevel(Logger logger, Level level) {
        if (logger != null) {
            logger.setLevel(level);
        }
    }

}
