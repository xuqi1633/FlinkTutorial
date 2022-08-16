package org.xq.trigger;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.xq.Event;
import org.xq.source.ClickSource;
import org.xq.window.UrlViewCountExample;

import java.sql.Timestamp;
import java.util.Calendar;

/**
 * @author xuqi
 */
public class TriggerExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Event>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                })
                )
                .keyBy(r -> r.url)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .trigger(new MyTrigger())
                .process(new WindowResult())
                .print();

        env.execute();
    }


    private static class MyTrigger extends Trigger<Event, TimeWindow> {

        @Override
        public TriggerResult onElement(Event element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            ValueState<Boolean> isFirstEvent = ctx.getPartitionedState(new ValueStateDescriptor<>("first-event", Types.BOOLEAN));

            if (isFirstEvent.value() == null) {
                for (long i = window.getStart(); i < window.getEnd(); i = i + 1000L) {
                    ctx.registerEventTimeTimer(i);
                }
                isFirstEvent.update(true);
            }
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.FIRE;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
            ValueState<Boolean> isFirstEvent = ctx.getPartitionedState(new ValueStateDescriptor<>("first-event", Types.BOOLEAN));
            isFirstEvent.clear();
        }
    }

    private static class WindowResult extends ProcessWindowFunction<Event, UrlViewCountExample.UrlViewCount, String, TimeWindow> {
        @Override
        public void process(String s, ProcessWindowFunction<Event, UrlViewCountExample.UrlViewCount, String, TimeWindow>.Context context, Iterable<Event> elements, Collector<UrlViewCountExample.UrlViewCount> out) throws Exception {
            s = s + " : " + new Timestamp(context.currentWatermark());
            out.collect(
                    new UrlViewCountExample.UrlViewCount(s, elements.spliterator().getExactSizeIfKnown(),
                            context.window().getStart(), context.window().getEnd())
            );
        }

    }
}
