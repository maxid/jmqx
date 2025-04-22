package plus.jmqx.broker.cluster;

import lombok.Builder;
import lombok.Data;

/**
 * 集群节点信息
 *
 * @author maxid
 * @since 2025/4/17 09:48
 */
@Data
@Builder
public class ClusterNode {
    /**
     * 别名
     */
    private String  alias;
    /**
     * host
     */
    private String  host;
    /**
     * 端口
     */
    private Integer port;
    /**
     * 命名空间
     */
    private String  namespace;
}
