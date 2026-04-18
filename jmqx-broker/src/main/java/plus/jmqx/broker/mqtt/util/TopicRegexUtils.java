package plus.jmqx.broker.mqtt.util;

/**
 * 主题正则工具
 *
 * @author maxid
 * @since 2025/4/16 15:00
 */
public class TopicRegexUtils {

    /**
     * 默认实例
     */
    public static final TopicRegexUtils instance = new TopicRegexUtils();

    /**
     * 将主题过滤器转换为正则表达式
     */
    public static String regexTopic(String topic) {
        if (topic.startsWith("$")) {
            topic = "\\" + topic;
        }
        return topic
                .replaceAll("/", "\\\\/")
                .replaceAll("\\+", "[^/]+")
                .replaceAll("#", "(.+)") + "$";
    }

    /**
     * 判断主题是否匹配过滤器
     */
    public boolean match(String sourcesTopic, String targetTopic) {
        if (sourcesTopic == null || "".equals(sourcesTopic) || targetTopic == null || "".equals(targetTopic)) {
            return false;
        }
        return sourcesTopic.matches(TopicRegexUtils.regexTopic(targetTopic));
    }

}
