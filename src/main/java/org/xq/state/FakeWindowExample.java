package org.xq.state;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.xq.Event;
import org.xq.source.ClickSource;

import java.sql.Timestamp;

/**
 * MapState
 * 使用 KeyedProcessFunction 模拟滚动窗口
 *
 * @author xuqi
 */
public class FakeWindowExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );

        // 统计每10s内，每个url的pv
        stream.keyBy(data -> data.url)
                .process(new FakeWindowResult(10 * 1000L))
                .print();

        env.execute();
    }

    private static class FakeWindowResult extends KeyedProcessFunction<String, Event, String> {

        // 声明状态，用 map 保存 pv 值（窗口 start，count）
        private MapState<Long, Long> windowPvState;

        private final Long windowSize;

        private FakeWindowResult(Long windowSize) {
            this.windowSize = windowSize;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            windowPvState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("window-pv", Long.class, Long.class));
        }

        @Override
        public void processElement(Event value, KeyedProcessFunction<String, Event, String>.Context ctx, Collector<String> out) throws Exception {
            // 每来一条数据，就根据时间戳判断属于哪个窗口
            long windowStart = value.timestamp / windowSize * windowSize;
            long windowEnd = windowStart + windowSize;

            // 注册 end-1 的定时器，窗口触发计算
            ctx.timerService().registerEventTimeTimer(windowEnd - 1);

            // 更新状态中的pv值
            if (windowPvState.contains(windowStart)) {
                Long count = windowPvState.get(windowStart);
                windowPvState.put(windowStart, count + 1);
            } else {
                windowPvState.put(windowStart, 1L);
            }
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            long windowEnd = timestamp + 1;
            long windowStart = windowEnd - windowSize;
            Long pv = windowPvState.get(windowStart);
            out.collect("url: " + ctx.getCurrentKey() + " 访问量: " + pv
                    + " 窗口: " + new Timestamp(windowStart) + " ~ " + new Timestamp(windowEnd));
            // 模拟窗口的销毁，清除 map 中的 key
            windowPvState.remove(windowStart);
        }
    }
}
