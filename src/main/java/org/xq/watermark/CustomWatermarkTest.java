package org.xq.watermark;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.xq.Event;
import org.xq.source.ClickSource;

/**
 * @author xuqi
 */
public class CustomWatermarkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(new CustomWatermarkStrategy())
                .print();

        env.execute();
    }

    private static class CustomWatermarkStrategy implements WatermarkStrategy<Event> {

        @Override
        public TimestampAssigner<Event> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            return new SerializableTimestampAssigner<Event>() {
                @Override
                public long extractTimestamp(Event event, long l) {
                    // 告诉程序数据源里的时间戳是哪一个字段
                    return event.timestamp;
                }
            };
        }

        @Override
        public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return new CustomPeriodicGenerator();
        }
    }

    /**
     * 周期性水位线生成器（Periodic Generator）
     */
    private static class CustomPeriodicGenerator implements WatermarkGenerator<Event> {

        // 延迟时间
        private final Long delayTime = 5000L;
        // 观察到的最大时间戳
        private Long maxTs = Long.MIN_VALUE + delayTime + 1L;

        // 每来一条数据就调用一次
        @Override
        public void onEvent(Event event, long l, WatermarkOutput watermarkOutput) {
            // 更新最大时间戳
            maxTs = Math.max(event.timestamp, maxTs);
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
            // 发射水位线，默认 200ms 调用一次
            watermarkOutput.emitWatermark(new Watermark(maxTs - delayTime - 1L));
        }
    }

    /**
     * 断点式水位线生成器（Punctuated Generator）
     */
    public static class CustomPunctuatedGenerator implements WatermarkGenerator<Event> {

        @Override
        public void onEvent(Event event, long eventTimestamp, WatermarkOutput output) {
            // 只有在遇到特定的 itemId 时，才发出水位线
            if ("Mary".equals(event.user)) {
                output.emitWatermark(new Watermark(event.timestamp - 1));
            }
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            // 不需要做任何事情，因为我们在 onEvent 方法中发射了水位线
        }
    }
}
