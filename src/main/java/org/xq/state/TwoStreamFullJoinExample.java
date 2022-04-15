package org.xq.state;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * ListState
 *
 * @author xuqi
 */
public class TwoStreamFullJoinExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        SingleOutputStreamOperator<Tuple3<String, String, Long>> stream1 = env
                .fromElements(
                        /*Tuple3.of("a", "stream-1", 1000L),
                        Tuple3.of("b", "stream-1", 2000L)*/

                        Tuple3.of("a", "stream-1", 1000L),
                        Tuple3.of("b", "stream-1", 1000L),
                        Tuple3.of("a", "stream-1", 2000L),
                        Tuple3.of("b", "stream-1", 2000L)
                )
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Tuple3<String, String, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple3<String, String, Long> t, long l) {
                                return t.f2;
                            }
                        })
                );

        SingleOutputStreamOperator<Tuple3<String, String, Long>> stream2 = env
                .fromElements(
                        /*Tuple3.of("a", "stream-2", 3000L),
                        Tuple3.of("b", "stream-2", 4000L)*/

                        Tuple3.of("a", "stream-2", 3000L),
                        Tuple3.of("b", "stream-2", 3000L),
                        Tuple3.of("a", "stream-2", 4000L),
                        Tuple3.of("b", "stream-2", 4000L)
                )
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple3<String, String, Long> t, long l) {
                                return t.f2;
                            }
                        })
                );

        stream1.keyBy(data -> data.f0)
                .connect(stream2.keyBy(data -> data.f0))
                .process(new CoProcessFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>() {

                    private ListState<Tuple3<String, String, Long>> stream1ListState;
                    private ListState<Tuple3<String, String, Long>> stream2ListState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        stream1ListState = getRuntimeContext().getListState(
                                new ListStateDescriptor<>("stream1-list", Types.TUPLE(Types.STRING, Types.STRING)));

                        stream2ListState = getRuntimeContext().getListState(
                                new ListStateDescriptor<>("stream2-list", Types.TUPLE(Types.STRING, Types.STRING)));
                    }

                    @Override
                    public void processElement1(Tuple3<String, String, Long> value, Context ctx, Collector<String> out) throws Exception {
                        stream1ListState.add(value);
                        System.out.println("stream2 len: " + stream2ListState.get().spliterator().estimateSize());
                        for (Tuple3<String, String, Long> right : stream2ListState.get()) {
                            out.collect("process1: " + value.toString() + right);
                        }
                    }

                    @Override
                    public void processElement2(Tuple3<String, String, Long> value, Context ctx, Collector<String> out) throws Exception {
                        stream2ListState.add(value);
                        System.out.println("stream1 len: " + stream1ListState.get().spliterator().estimateSize());
                        for (Tuple3<String, String, Long> left : stream1ListState.get()) {
                            out.collect("process2: " + value.toString() + left);
                        }
                    }
                })
                .print();


        env.execute();
    }
}
