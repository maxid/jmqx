package plus.jmqx.broker;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import plus.jmqx.broker.support.ClusterStressTestSupport;
import plus.jmqx.broker.support.IntegrationTestLogSupport;
import plus.jmqx.broker.support.StressConfig;
import plus.jmqx.broker.support.StressTestSupport;

import java.util.UUID;

/**
 * 集群消息吞吐压测
 */
@Slf4j
@EnabledIfSystemProperty(named = "jmqx.integration.tests", matches = "true")
public class ClusterStressTest {

    @Test
    void testClusterStress() throws Exception {
        IntegrationTestLogSupport.setClusterStressLogContext();

        StressConfig stress = StressTestSupport.loadStressConfig();
        String namespace = "jmqx-cluster-stress-" + UUID.randomUUID().toString().split("-")[0];
        String clusterUrls = "127.0.0.1:7771,127.0.0.1:7772";

        ClusterStressTestSupport.runClusterStress(stress, namespace, clusterUrls);
    }
}
