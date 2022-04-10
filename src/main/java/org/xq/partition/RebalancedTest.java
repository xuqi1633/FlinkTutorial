package org.xq.partition;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.xq.Event;
import org.xq.source.ClickSource;

/**
 * @author xuqi
 */
public class RebalancedTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.addSource(new ClickSource());

        stream.rebalance().print("rebalanced").setParallelism(4);

        env.execute();
    }
}
