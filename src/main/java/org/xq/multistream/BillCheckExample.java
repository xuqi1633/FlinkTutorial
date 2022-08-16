package org.xq.multistream;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 当定时器timer触发时，执行回调函数onTimer()。processElement()方法和 onTimer()方法是同步（不是异步）方法，这样可以避免并发访问和操作状态。先调用 processElement 后调用 onTimer
 * <p>
 * 针对每一个key和timestamp，只能注册一个定期器。也就是说，每一个key可以注册多个定时器，但在每一个时间戳只能注册一个定时器。KeyedProcessFunction默认将所有定时器的时间戳放在一个优先队列中，每次水位线改变后，都会对队列中小于等于水位线的定时器进行调用。
 * <p>
 * 在Flink做检查点操作时，定时器也会被保存到状态后端中。
 * <p>
 * 在 Flink 中，只有“按键分区流”KeyedStream 才支持设置定时器的操作
 *
 * @author xuqi
 */
public class BillCheckExample {
    public static void main(String[] args) throws Exception {
        // StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);

        // 来自 app 的支付日志
        SingleOutputStreamOperator<Tuple3<String, String, Long>> appStream = env
                /*.fromElements(
                        Tuple3.of("order-0", "app", 6500L),
                        Tuple3.of("order-1", "app", 1000L),
                        Tuple3.of("order-2", "app", 2000L))
                .map(data -> data.f0+" "+data.f1+" "+data.f2)*/

                .socketTextStream("localhost", 7777)
                .map(data -> {
                    String[] fields = data.split(" ");
                    return Tuple3.of(fields[0].trim(), fields[1].trim(), Long.parseLong(fields[2].trim()));
                }).returns(Types.TUPLE(Types.STRING, Types.STRING, Types.LONG))

                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Tuple3<String, String, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                                return element.f2;
                            }
                        })
                );
        // 来自第三方支付平台的支付日志
        SingleOutputStreamOperator<Tuple4<String, String, String, Long>> thirdpartStream = env
                /*.fromElements(
                        Tuple4.of("order-0", "third-party", "success", 7000L),
                        Tuple4.of("order-1", "third-party", "success", 6600L),
                        Tuple4.of("order-3", "third-party", "success", 4000L))
                .map(data -> data.f0+" "+data.f1+" "+data.f2+" "+data.f3)*/

                .socketTextStream("localhost", 9999)
                .map(data -> {
                    String[] fields = data.split(" ");
                    return Tuple4.of(fields[0].trim(), fields[1].trim(), fields[2].trim(), Long.parseLong(fields[3].trim()));
                }).returns(Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.LONG))

                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Tuple4<String, String, String, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple4<String, String, String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple4<String, String, String, Long> element, long recordTimestamp) {
                                return element.f3;
                            }
                        })
                );

        // 检测同一支付单在两条流中是否匹配，不匹配就报警
        appStream.connect(thirdpartStream)
                .keyBy(data -> data.f0, data -> data.f0)
                .process(new OrderMatchResult())
                .print();

        env.execute();
    }

    private static class OrderMatchResult extends
            CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String> {

        // 定义状态变量，用来保存已经到达的事件
        private ValueState<Tuple3<String, String, Long>> appEventState;
        private ValueState<Tuple4<String, String, String, Long>> thirdPartyEventState;

        @Override
        public void open(Configuration parameters) throws Exception {
            appEventState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("app-event", Types.TUPLE(Types.STRING, Types.STRING, Types.LONG)));

            thirdPartyEventState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("thirdParty-event", Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.LONG)));
        }

        @Override
        public void processElement1(Tuple3<String, String, Long> value, Context ctx, Collector<String> out) throws Exception {
            // 看另一条流中事件是否来过
            if (thirdPartyEventState.value() != null) {
                out.collect("对账成功：" + value + " " + thirdPartyEventState.value() + "  water mark: " + ctx.timerService().currentWatermark());
                Thread.sleep(10000);

                // 清空状态
                thirdPartyEventState.clear();
            } else {
                // 更新状态
                appEventState.update(value);
                // 注册一个 5s 后的定时器，开始等待另一条流的事件
                ctx.timerService().registerEventTimeTimer(value.f2 + 5000L);
            }
        }

        @Override
        public void processElement2(Tuple4<String, String, String, Long> value, Context ctx, Collector<String> out) throws Exception {
            if (appEventState.value() != null) {
                out.collect("对账成功：" + value + " " + appEventState.value() + "  water mark: " + ctx.timerService().currentWatermark());
                Thread.sleep(10000);
                appEventState.clear();
            } else {
                thirdPartyEventState.update(value);
                ctx.timerService().registerEventTimeTimer(value.f3 + 5000L);
            }
        }

        @Override
        public void onTimer(long timestamp, CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定时器触发，判断状态，如果某个状态不为空，说明另一条流中事件没来
            if (appEventState.value() != null) {
                System.out.println("TS: " + timestamp);
                out.collect("对账失败：" + appEventState.value() + " " + "第三方支付平台信息未到" + "  water mark: " + ctx.timerService().currentWatermark());
            }
            if (thirdPartyEventState.value() != null) {
                System.out.println("TS: " + timestamp);
                out.collect("对账失败：" + thirdPartyEventState.value() + " " + "app信息未到" + "  water mark: " + ctx.timerService().currentWatermark());
            }
            System.out.println("TS ------: " + timestamp);
            appEventState.clear();
            thirdPartyEventState.clear();
        }
    }
}