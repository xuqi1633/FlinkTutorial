package org.xq.multistream;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.xq.Event;
import org.xq.source.ClickSource;

/**
 * @author xuqi
 */
public class SplitStreamByOutputTag {

    private static OutputTag<Tuple3<String, String, Long>> maryTag = new OutputTag<Tuple3<String, String, Long>>("Mary-pv") {
    };
    private static OutputTag<Tuple3<String, String, Long>> BobTag = new OutputTag<Tuple3<String, String, Long>>("Bob-pv") {
    };

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.addSource(new ClickSource());

        SingleOutputStreamOperator<Event> process = stream.process(new ProcessFunction<Event, Event>() {
            @Override
            public void processElement(Event value, ProcessFunction<Event, Event>.Context ctx, Collector<Event> out) throws Exception {
                if ("Mary".equals(value.user)) {
                    ctx.output(maryTag, Tuple3.of(value.user, value.url, value.timestamp));
                } else if ("Bob".equals(value.user)) {
                    ctx.output(BobTag, Tuple3.of(value.user, value.url, value.timestamp));
                } else {
                    out.collect(value);
                }
            }
        });

        process.getSideOutput(maryTag).print("Mary pv");
        process.getSideOutput(BobTag).print("Bob pv");
        process.print("Other pv");

        env.execute();
    }
}
