package org.xq.transfer;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.xq.Event;
import org.xq.source.ClickSource;

/**
 * @author xuqi
 */
public class TransReduceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(5);

        env.addSource(new ClickSource())
                // 将 Event 数据类型转换为元组类型
                .map(new MapFunction<Event, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(Event event) throws Exception {
                        return Tuple2.of(event.user, 1L);
                    }
                })
                // 使用用户名来进行分流
                .keyBy(r -> r.f0)
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) {
                        System.out.println(value1 + "------" + value2);
                        // 每到一条数据，用户pv的统计值加 1
                        // 注意：这里只有相同的key才会累加
                        return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                    }
                })
                // 为每一条数据分配同一个key，将聚合结果发送到同一条流中
                // 注意：这里发送到下游所有数据的key都一样
                .keyBy(r -> true)
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) {
                        // 将累加器更新为当前最大的pv统计值，然后向下游发送累加器的值
                        return value1.f1 > value2.f1 ? value1 : value2;
                    }
                })
                .print();

        env.execute();
    }
}
