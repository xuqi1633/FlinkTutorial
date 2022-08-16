package org.xq.transfer;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.xq.Event;

/**
 * @author xuqi
 */
public class TransFunctionUDFTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> clicks = env.fromElements(
                new Event("Marry", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

        SingleOutputStreamOperator<Event> stream = clicks.filter(new FlinkFilter());

        stream.print();

        env.execute();
    }

    private static class FlinkFilter implements org.apache.flink.api.common.functions.FilterFunction<Event> {
        @Override
        public boolean filter(Event event) throws Exception {
            return event.url.contains("home");
        }
    }
}
