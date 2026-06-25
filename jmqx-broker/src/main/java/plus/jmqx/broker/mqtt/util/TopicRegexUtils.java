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
     *
     * @param topic 主题过滤器
     * @return 正则表达式字符串
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
     *
     * @param sourcesTopic 源主题
     * @param targetTopic  目标主题过滤器
     * @return 是否匹配
     */
    public boolean match(String sourcesTopic, String targetTopic) {
        if (sourcesTopic == null || "".equals(sourcesTopic) || targetTopic == null || "".equals(targetTopic)) {
            return false;
        }
        return sourcesTopic.matches(TopicRegexUtils.regexTopic(targetTopic));
    }

}
