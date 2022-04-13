package org.xq.process;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.xq.Event;
import org.xq.source.ClickSource;
import org.xq.window.UrlViewCountExample;

import java.sql.Timestamp;
import java.util.ArrayList;

/**
 * @author xuqi
 */
public class KeyedProcessTopN {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setString("jobmanager.memory.process.size", "1024mb");
        configuration.setString("taskmanager.memory.process.size", "1024mb");
        configuration.setString("taskmanager.numberOfTaskSlots", "4");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        // StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        SingleOutputStreamOperator<Event> eventStream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );

        // 需要按照 url 分组，求出每个 url 的访问量
        SingleOutputStreamOperator<UrlViewCountExample.UrlViewCount> urlCountStream = eventStream
                .keyBy(data -> data.url)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new UrlViewCountAgg(), new UrlCountResult());

        // 对结果中同一个窗口的统计数据，进行排序处理
        SingleOutputStreamOperator<String> result = urlCountStream.keyBy(data -> data.windowEnd)
                .process(new TopN(2));

        result.print("result");

        env.execute();
    }

    private static class UrlViewCountAgg implements AggregateFunction<Event, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Event value, Long accumulator) {
            return accumulator + 1L;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
    }

    private static class UrlCountResult extends ProcessWindowFunction<Long, UrlViewCountExample.UrlViewCount, String, TimeWindow> {
        @Override
        public void process(String url, ProcessWindowFunction<Long, UrlViewCountExample.UrlViewCount, String, TimeWindow>.Context context, Iterable<Long> elements, Collector<UrlViewCountExample.UrlViewCount> out) throws Exception {
            long start = context.window().getStart();
            long end = context.window().getEnd();
            out.collect(new UrlViewCountExample.UrlViewCount(url, elements.iterator().next(), start, end));
        }
    }

    private static class TopN extends KeyedProcessFunction<Long, UrlViewCountExample.UrlViewCount, String> {
        private final Integer n;

        private ListState<UrlViewCountExample.UrlViewCount> urlViewCountListState;

        public TopN(Integer n) {
            this.n = n;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            // 从环境中获取列表状态句柄
            urlViewCountListState = getRuntimeContext().getListState(new ListStateDescriptor<>("url-view-count-list",
                    Types.POJO(UrlViewCountExample.UrlViewCount.class)));
        }

        @Override
        public void processElement(UrlViewCountExample.UrlViewCount value, KeyedProcessFunction<Long, UrlViewCountExample.UrlViewCount, String>.Context ctx, Collector<String> out) throws Exception {
            // 将count数据添加到列表状态中，保存起来
            urlViewCountListState.add(value);
            // 注册 window end + 1ms 后的定时器，等待所有数据到齐开始排序
            ctx.timerService().registerEventTimeTimer(ctx.getCurrentKey() + 1);
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Long, UrlViewCountExample.UrlViewCount, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            ArrayList<UrlViewCountExample.UrlViewCount> urlViewCountArrayList = new ArrayList<>();
            for (UrlViewCountExample.UrlViewCount urlViewCount : urlViewCountListState.get()) {
                urlViewCountArrayList.add(urlViewCount);
            }

            // 清空状态，释放资源
            urlViewCountListState.clear();

            urlViewCountArrayList.sort((o1, o2) -> o2.count.intValue() - o1.count.intValue());

            // 取前两名，构建输出结果
            StringBuilder result = new StringBuilder();
            result.append("\n========================================\n");
            result.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append("\n");

            int len = Math.min(this.n, urlViewCountArrayList.size());
            for (int i = 0; i < len; i++) {
                UrlViewCountExample.UrlViewCount urlViewCount = urlViewCountArrayList.get(i);
                String info = "No." + (i + 1) + " "
                        + "url：" + urlViewCount.url + " "
                        + "浏览量：" + urlViewCount.count + "\n";
                result.append(info);
            }
            result.append("========================================\n");
            out.collect(result.toString());
        }
    }
}
