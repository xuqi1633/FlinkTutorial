package org.xq.transfer;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.xq.Event;

/**
 * @author xuqi
 */
public class ReturnTypeResolve {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> clicks = env.fromElements(
                new Event("Marry", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

        // 使用map函数也会出现类似的问题，以下代码会报错
        // SingleOutputStreamOperator<Tuple2<String, Long>> stream3 = clicks.map(event -> Tuple2.of(event.user, 1L));



        /*----------------想要转换成二元组类型，需要进行以下处理--------------------*/

        // 1. 使用显示的 .returns(...)
        SingleOutputStreamOperator<Tuple2<String, Long>> stream3 = clicks
                .map(event -> Tuple2.of(event.user, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG));

        stream3.print();


        // 2. 使用类来替代 Lambda 表达式
        clicks.map(new MyTuple2Mapper()).print();

        // 3. 使用匿名类来代替 Lambda 表达式
        clicks.map(new MapFunction<Event, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(Event event) throws Exception {
                return Tuple2.of(event.user, 1L);
            }
        }).print();


        env.execute();
    }

    private static class MyTuple2Mapper implements MapFunction<Event, Tuple2<String, Long>> {
        @Override
        public Tuple2<String, Long> map(Event event) throws Exception {
            return Tuple2.of(event.user, 1L);
        }
    }
}
