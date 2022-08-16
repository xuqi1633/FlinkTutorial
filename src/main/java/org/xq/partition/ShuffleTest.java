package org.xq.partition;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.xq.Event;
import org.xq.source.ClickSource;

/**
 * @author xuqi
 */
public class ShuffleTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取数据源，并行度为 1
        DataStreamSource<Event> stream = env.addSource(new ClickSource());

        // 经洗牌后打印输出，并行度为 4
        stream.shuffle().print("shuffle").setParallelism(4);

        env.execute();
    }
}
