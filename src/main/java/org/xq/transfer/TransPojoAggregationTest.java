package org.xq.transfer;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.xq.Event;

import java.util.ArrayList;

/**
 * minBy()：与 min()类似，在输入流上针对指定字段求最小值。不同的是，min()只计
 * 算指定字段的最小值，其他字段会保留最初第一个数据的值；而 minBy()则会返回包
 * 含字段最小值的整条数据。
 * maxBy() 同理
 * <p>
 * 数据流的类型是 POJO 类，那么就只能通过字段名称来指定，不能通过位置来指定
 *
 * @author xuqi
 */
public class TransPojoAggregationTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        ArrayList<Event> clicks = new ArrayList<>();
        clicks.add(new Event("Mary", "./home", 1000L));
        clicks.add(new Event("Bob", "./cart", 4000L));

        clicks.add(new Event("Mary", "./home???", 3000L));
        clicks.add(new Event("Bob", "./cart???", 2000L));


        DataStreamSource<Event> source = env.fromCollection(clicks);

        source.keyBy(r -> r.user)
                .sum("timestamp")
                .print("sum: ");
        source.keyBy(r -> r.user).max("timestamp").print("max: ");
        source.keyBy(r -> r.user).min("timestamp").print("min: ");
        source.keyBy(r -> r.user).maxBy("timestamp").print("maxby: ");
        source.keyBy(r -> r.user).minBy("timestamp").print("minby: ");

    }
}
