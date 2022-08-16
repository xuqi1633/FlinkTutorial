package org.xq.multistream;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.xq.Event;

import java.time.Duration;

/**
 * @author xuqi
 */
public class UnionExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream1 = env
                .socketTextStream("localhost", 7777)
                .map(data -> {
                    String[] field = data.split(",");
                    return new Event(field[0].trim(), field[1].trim(), Long.valueOf(field[2].trim()));
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );
        stream1.print("stream1");

        SingleOutputStreamOperator<Event> stream2 =
                env.socketTextStream("localhost", 9999)
                        .map(data -> {
                            String[] field = data.split(",");
                            return new Event(field[0].trim(), field[1].trim(), Long.valueOf(field[2].trim()));
                        })
                        .assignTimestampsAndWatermarks(WatermarkStrategy
                                .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                })
                        );
        stream2.print("stream2");

        // 合并两条流
        stream1.union(stream2)
                .process(new ProcessFunction<Event, String>() {
                    @Override
                    public void processElement(Event value, ProcessFunction<Event, String>.Context ctx, Collector<String> out) throws Exception {
                        out.collect(" 水位线：" + ctx.timerService().currentWatermark());
                    }
                })
                .print();

        env.execute();
    }
}
