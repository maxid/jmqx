package plus.jmqx.broker;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import plus.jmqx.broker.mqtt.MqttConfiguration;

class BootstrapTest {
    @Test
    void cluster01() throws Exception {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        loggerContext.getLogger("root").setLevel(Level.INFO);
        loggerContext.getLogger("plus.jmqx.broker").setLevel(Level.DEBUG);
        loggerContext.getLogger("reactor.netty").setLevel(Level.DEBUG);
        MqttConfiguration config = new MqttConfiguration();
        Bootstrap bootstrap = new Bootstrap(config);
        bootstrap.start().block();
        Thread.sleep(3600 * 1000);
        bootstrap.shutdown();
    }
}