package plus.jmqx.broker;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class BootstrapTest {
    @Test
    void cluster01() throws Exception {
        new Bootstrap().start().block();
        Thread.sleep(10000000);
    }
}