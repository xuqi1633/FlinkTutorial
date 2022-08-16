package org.xq.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.xq.Event;

import java.util.ArrayList;

/**
 * @author xuqi
 */
public class SourceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        ArrayList<Event> clicks = new ArrayList<>();
        clicks.add(new Event("Mary", "./home", 1000L));
        clicks.add(new Event("Bob", "./cart", 2000L));

        // 从集合中读取数据
        DataStreamSource<Event> stream = env.fromCollection(clicks);
        // 从文件中读取数据
        // DataStreamSource<String> stream = env.readTextFile("path");
        // 从 socket 读取数据
        // DataStreamSource<String> stream = env.socketTextStream("host", 9999);

        stream.print();

        env.execute();
    }
}
