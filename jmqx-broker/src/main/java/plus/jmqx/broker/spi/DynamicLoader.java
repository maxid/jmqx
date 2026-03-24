package plus.jmqx.broker.spi;

import java.util.Optional;
import java.util.ServiceLoader;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * SPI 加载器
 *
 * @author maxid
 * @since 2025/4/15 09:30
 */
public class DynamicLoader {

    /**
     * 工具类禁止实例化。
     */
    private DynamicLoader() {
    }

    /**
     * 查找第一个实现。
     *
     * @param clazz SPI 接口类型
     * @param <T>   泛型类型
     * @return 第一个实现
     */
    public static <T> Optional<T> findFirst(Class<T> clazz) {
        ServiceLoader<T> load = ServiceLoader.load(clazz);
        return StreamSupport.stream(load.spliterator(), false).findFirst();
    }

    /**
     * 查找符合条件的第一个实现。
     *
     * @param clazz     SPI 接口类型
     * @param predicate 过滤条件
     * @param <T>       泛型类型
     * @return 第一个实现
     */
    public static <T> Optional<T> findFirst(Class<T> clazz, Predicate<? super T> predicate) {
        ServiceLoader<T> load = ServiceLoader.load(clazz);
        Stream<T> stream = StreamSupport.stream(load.spliterator(), false);
        return stream.filter(predicate).findFirst();
    }

    /**
     * 查找全部实现。
     *
     * @param clazz SPI 接口类型
     * @param <T>   泛型类型
     * @return 实现流
     */
    public static <T> Stream<T> findAll(Class<T> clazz) {
        ServiceLoader<T> load = ServiceLoader.load(clazz);
        return StreamSupport.stream(load.spliterator(), false);
    }

    /**
     * 查找符合条件的全部实现。
     *
     * @param clazz     SPI 接口类型
     * @param predicate 过滤条件
     * @param <T>       泛型类型
     * @return 实现流
     */
    public static <T> Stream<T> findAll(Class<T> clazz, Predicate<? super T> predicate) {
        ServiceLoader<T> load = ServiceLoader.load(clazz);
        return StreamSupport.stream(load.spliterator(), false).filter(predicate);
    }

}
