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

    public TreeNode(String topic) {
        this.topic = topic;
    }

    private final String SINGLE_SYMBOL = "+";

    private final String MULTI_SYMBOL = "#";

    public boolean addSubscribeTopic(SubscribeTopic subscribeTopic) {
        String[] topics = TopicUtils.splitTopic(subscribeTopic.getTopicFilter());
        return addIndex(subscribeTopic, topics, 0);
    }

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

    public List<SubscribeTopic> getSubscribeByTopic(String topicFilter) {
        String[] topics = TopicUtils.splitTopic(topicFilter);
        return searchTree(topics);
    }

    private List<SubscribeTopic> searchTree(String[] topics) {
        LinkedList<SubscribeTopic> subscribeTopicList = new LinkedList<>();
        loadTreeSubscribes(this, subscribeTopicList, topics, 0);
        return subscribeTopicList;
    }

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

    public Set<SubscribeTopic> getAllSubscribesTopic() {
        return getTreeSubscribesTopic(this);
    }

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
