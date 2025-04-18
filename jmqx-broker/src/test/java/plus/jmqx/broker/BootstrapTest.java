package plus.jmqx.broker;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.*;

class BootstrapTest {
    @Test
    void cluster01() throws Exception {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        loggerContext.getLogger("root").setLevel(Level.INFO);
        loggerContext.getLogger("plus.jmqx.broker").setLevel(Level.DEBUG);
        loggerContext.getLogger("reactor.netty").setLevel(Level.DEBUG);
        new Bootstrap().startAwait();
        //Thread.sleep(10000000);
    }
}