package org.xq.partition;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.xq.Event;
import org.xq.source.ClickSource;

/**
 * @author xuqi
 */
public class BroadcastTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取数据源，并行度为 1
        DataStreamSource<Event> stream = env.addSource(new ClickSource());

        // 经广播后打印输出，并行度为 4
        stream.broadcast().print("broadcast").setParallelism(4);

        // 全局分区也是一种特殊的分区方式。这种做法非常极端，通过调用.global()方法，会将所
        // 有的输入流数据都发送到下游算子的第一个并行子任务中去。这就相当于强行让下游任务并行
        // 度变成了 1，所以使用这个操作需要非常谨慎，可能对程序造成很大的压力。
        stream.global().print("broadcast").setParallelism(4);

        env.execute();
    }
}
