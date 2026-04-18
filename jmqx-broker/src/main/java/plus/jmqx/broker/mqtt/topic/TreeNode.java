package plus.jmqx.broker.mqtt.topic;

import lombok.Getter;
import lombok.Setter;
import plus.jmqx.broker.mqtt.util.TopicUtils;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * 将 TOPIC 转换为树结构
 *
 * @author maxid
 * @since 2025/4/16 10:18
 */
@Getter
@Setter
public class TreeNode {

    /**
     * MQTT 主题
     */
    private final String topic;

    private int subscribeTopicNumber;

    private Set<SubscribeTopic> subscribes = ConcurrentHashMap.newKeySet();

    private Map<String, TreeNode> childNodes = new ConcurrentHashMap<>();

    private volatile TreeNode singleNode;

    private volatile TreeNode multiNode;

    /**
     * 创建树节点
     *
     * @param topic 主题片段
     */
    public TreeNode(String topic) {
        this.topic = topic;
    }

    private final String SINGLE_SYMBOL = "+";

    private final String MULTI_SYMBOL = "#";

    /**
     * 添加订阅到树结构
     *
     * @param subscribeTopic 订阅对象
     * @return 是否新增成功
     */
    public boolean addSubscribeTopic(SubscribeTopic subscribeTopic) {
        String[] topics = TopicUtils.splitTopic(subscribeTopic.getTopicFilter());
        return addIndex(subscribeTopic, topics, 0);
    }

    /**
     * 递归添加订阅节点
     *
     * @param subscribeTopic 订阅对象
     * @param topics         主题片段
     * @param index          当前索引
     * @return 是否新增成功
     */
    private boolean addIndex(SubscribeTopic subscribeTopic, String[] topics, Integer index) {
        String lastTopic = topics[index];
        TreeNode treeNode = childNodes.computeIfAbsent(lastTopic, tp -> new TreeNode(lastTopic));
        if (SINGLE_SYMBOL.equals(lastTopic)) {
            this.singleNode = treeNode;
        } else if (MULTI_SYMBOL.equals(lastTopic)) {
            this.multiNode = treeNode;
        }
        if (index == topics.length - 1) {
            return treeNode.addTreeSubscribe(subscribeTopic);
        } else {
            return treeNode.addIndex(subscribeTopic, topics, index + 1);
        }
    }

    private boolean addTreeSubscribe(SubscribeTopic subscribeTopic) {
        return subscribes.add(subscribeTopic);
    }

    /**
     * 根据主题获取匹配订阅
     *
     * @param topicFilter 主题
     * @return 订阅列表
     */
    public List<SubscribeTopic> getSubscribeByTopic(String topicFilter) {
        String[] topics = TopicUtils.splitTopic(topicFilter);
        return searchTree(topics);
    }

    /**
     * 搜索匹配订阅
     *
     * @param topics 主题片段
     * @return 订阅列表
     */
    private List<SubscribeTopic> searchTree(String[] topics) {
        LinkedList<SubscribeTopic> subscribeTopicList = new LinkedList<>();
        loadTreeSubscribes(this, subscribeTopicList, topics, 0);
        return subscribeTopicList;
    }

    /**
     * 加载匹配订阅到结果集中
     *
     * @param treeNode        当前节点
     * @param subscribeTopics 结果集合
     * @param topics          主题片段
     * @param index           当前索引
     */
    private void loadTreeSubscribes(TreeNode treeNode, LinkedList<SubscribeTopic> subscribeTopics, String[] topics, Integer index) {
        String lastTopic = topics[index];
        TreeNode moreTreeNode = treeNode.multiNode;
        if (moreTreeNode != null) {
            subscribeTopics.addAll(moreTreeNode.getSubscribes());
        }
        if (index == topics.length - 1) {
            TreeNode localTreeNode = treeNode.getChildNodes().get(lastTopic);
            if (localTreeNode != null) {
                Set<SubscribeTopic> subscribes = localTreeNode.getSubscribes();
                if (subscribes != null && !subscribes.isEmpty()) {
                    subscribeTopics.addAll(subscribes);
                }
            }
            TreeNode oneTreeNode = treeNode.singleNode;
            if (oneTreeNode != null) {
                Set<SubscribeTopic> subscribes = oneTreeNode.getSubscribes();
                if (subscribes != null && !subscribes.isEmpty()) {
                    subscribeTopics.addAll(subscribes);
                }
            }
        } else {
            TreeNode oneTreeNode = treeNode.singleNode;
            if (oneTreeNode != null) {
                loadTreeSubscribes(oneTreeNode, subscribeTopics, topics, index + 1);
            }
            TreeNode node = treeNode.getChildNodes().get(lastTopic);
            if (node != null) {
                loadTreeSubscribes(node, subscribeTopics, topics, index + 1);
            }
        }
    }

    public boolean removeSubscribeTopic(SubscribeTopic subscribeTopic) {
        TreeNode node = this;
        String[] topics = TopicUtils.splitTopic(subscribeTopic.getTopicFilter());
        for (String topic : topics) {
            if (node != null) {
                node = node.getChildNodes().get(topic);
            }
        }
        if (node != null) {
            Set<SubscribeTopic> subscribeTopics = node.getSubscribes();
            if (subscribeTopics != null) {
                return subscribeTopics.remove(subscribeTopic);
            }
        }
        return false;
    }

    /**
     * 获取全部订阅集合
     *
     * @return 订阅集合
     */
    public Set<SubscribeTopic> getAllSubscribesTopic() {
        return getTreeSubscribesTopic(this);
    }

    /**
     * 递归获取所有订阅
     *
     * @param node 节点
     * @return 订阅集合
     */
    private Set<SubscribeTopic> getTreeSubscribesTopic(TreeNode node) {
        Set<SubscribeTopic> allSubscribeTopics = new HashSet<>();
        allSubscribeTopics.addAll(node.getSubscribes());
        allSubscribeTopics.addAll(node.getChildNodes()
                .values()
                .stream()
                .flatMap(treeNode -> treeNode.getTreeSubscribesTopic(treeNode).stream())
                .collect(Collectors.toSet()));
        return allSubscribeTopics;
    }

}
