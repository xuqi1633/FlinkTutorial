package org.xq.transfer;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.xq.Event;

/**
 * 富函数类可以获取运行环境的上下文，并拥有一些生命周期方法，所以可以实现
 * 更复杂的功能。
 *
 * @author xuqi
 */
public class RichFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<Event> clicks = env.fromElements(
                new Event("Marry", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=1", 5 * 1000L),
                new Event("Cary", "./home", 60 * 1000L)
        );


        // 将点击事件转换成长整型的时间戳输出
        clicks.map(new RichMapFunction<Event, Long>() {

                    /**
                     * Rich Function 的初始化方法，也就是开启一个算子的生命周期。
                     * 当一个算子的实际工作方法例如 map() 或者 filter() 方法被调用之前，open() 会首先被调用。
                     * 所以像IO的创建，配置文件的读取等等这样一次性的工作，都适合在 open() 方法中完成
                     *
                     * 注意：这里的声明周期方法，对于一个并行子任务来说只会调用一次，也就是每个 subTask 调用一次
                     */
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        System.out.println("索引为 " + getRuntimeContext().getIndexOfThisSubtask() + " 的任务开始");
                    }

                    @Override
                    public Long map(Event event) {
                        return event.timestamp;
                    }

                    @Override
                    public void close() throws Exception {
                        super.close();
                        System.out.println("索引为 " + getRuntimeContext().getIndexOfThisSubtask() + " 的任务结束");
                    }
                })
                .print();

        env.execute();
    }
}
