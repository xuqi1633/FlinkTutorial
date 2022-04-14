package org.xq.multistream;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.xq.Event;
import org.xq.source.ClickSource;

/**
 * @author xuqi
 */
public class SplitStreamByFilter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.addSource(new ClickSource());

        SingleOutputStreamOperator<Event> maryStream = stream.filter((Event value) -> "Mary".equals(value.user));
        SingleOutputStreamOperator<Event> bobStream = stream.filter((Event value) -> "Bob".equals(value.user));
        SingleOutputStreamOperator<Event> otherStream = stream.filter((Event value) -> !"Mary".equals(value.user) && !"Bob".equals(value.user));

        maryStream.print("Mary pv");
        bobStream.print("Bob pv");
        otherStream.print("else pv");

        env.execute();
    }
}
