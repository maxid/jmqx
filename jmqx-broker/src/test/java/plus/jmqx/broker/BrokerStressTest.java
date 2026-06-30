package plus.jmqx.broker;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import plus.jmqx.broker.mqtt.MqttConfiguration;
import plus.jmqx.broker.support.IntegrationTestLogSupport;
import plus.jmqx.broker.support.StressConfig;
import plus.jmqx.broker.support.StressResult;
import plus.jmqx.broker.support.StressTestSupport;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Broker 消息吞吐压测
 *
 * @author maxid
 * @since 2026/6/27
 */
@Slf4j
@EnabledIfSystemProperty(named = "jmqx.integration.tests", matches = "true")
class BrokerStressTest {

    @Test
    void testBrokerStress() throws Exception {
        IntegrationTestLogSupport.setBrokerStressLogContext();

        StressConfig stress = StressTestSupport.loadStressConfig();
        int mqttPort = StressTestSupport.resolvePort(stress.port);
        stress.port = mqttPort;
        AtomicLong dispatchReceived = new AtomicLong();
        MqttConfiguration config = StressTestSupport.brokerStressConfig(mqttPort);
        Bootstrap bootstrap = new Bootstrap(config, StressTestSupport.countingDispatcher(dispatchReceived));
        bootstrap.start().block();

        try {
            StressResult result = StressTestSupport.runBrokerStress(stress, dispatchReceived);
            StressTestSupport.logStressResult("broker", stress, result, dispatchReceived.get());
        } finally {
            bootstrap.shutdown();
        }
    }

}
