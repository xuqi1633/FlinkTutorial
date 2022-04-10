package org.xq.watermark;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.xq.Event;
import org.xq.source.ClickSource;

import java.time.Duration;

/**
 * 以下两种水位线生成器都是周期性生成水位线的
 *
 * @author xuqi
 */
public class WatermarkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.addSource(new ClickSource());
        // 有序流
        stream.assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<Event>() {
                                            @Override
                                            public long extractTimestamp(Event element, long recordTimestamp) {
                                                return element.timestamp;
                                            }
                                        }
                                )
                )
                .print();

        // 乱序流
        stream.assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                // 针对乱序流插入水位线，延迟时间设置为5s
                                .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<Event>() {
                                            // 抽取时间戳的逻辑
                                            @Override
                                            public long extractTimestamp(Event event, long l) {
                                                return event.timestamp;
                                            }
                                        }
                                )
                )
                .print();

        env.execute();
    }
}
