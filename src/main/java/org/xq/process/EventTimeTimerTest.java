package org.xq.process;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.xq.Event;

/**
 * @author xuqi
 */
public class EventTimeTimerTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new CustomSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );

        stream.keyBy(data -> true)
                .process(new KeyedProcessFunction<Boolean, Event, String>() {
                    @Override
                    public void processElement(Event value, KeyedProcessFunction<Boolean, Event, String>.Context ctx, Collector<String> out) throws Exception {
                        out.collect("数据到达，时间戳为：" + ctx.timestamp());
                        out.collect("数据到达，水位线为：" + ctx.timerService().currentWatermark() + "\n----------------分割线----------------");
                        ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 10 * 1000L);
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<Boolean, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect("定时器触发，触发时间：" + timestamp + "   水位线为：" + ctx.timerService().currentWatermark());
                    }
                })
                .print();

        env.execute();
    }

    private static class CustomSource implements SourceFunction<Event> {
        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
            ctx.collect(new Event("Marry", "./home", 1000L));
            Thread.sleep(5000L);

            ctx.collect(new Event("Marry", "./home", 11 * 1000L));
            Thread.sleep(5000L);

            ctx.collect(new Event("Marry", "./home", 11 * 1000 + 1L));
            Thread.sleep(5000L);
        }

        @Override
        public void cancel() {

        }
    }
}
