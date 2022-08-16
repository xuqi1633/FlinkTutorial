package org.xq.partition;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * 由于 rebalance 是所有分区数据的“重新平衡”，当 TaskManager 数据量较多时，这种跨节
 * 点的网络传输必然影响效率；而如果我们配置的 task slot 数量合适，用 rescale 的方式进行“局
 * 部重缩放”，就可以让数据只在当前 TaskManager 的多个 slot 之间重新分配，从而避免了网络
 * 传输带来的损耗。
 * <p>
 * rebalance 和 rescale 的根本区别在于任务之间的连接机制不同。rebalance
 * 将会针对所有上游任务（发送数据方）和所有下游任务（接收数据方）之间建立通信通道，这
 * 是一个笛卡尔积的关系；而 rescale 仅仅针对每一个任务和下游对应的部分任务之间建立通信
 * 通道，节省了很多资源。
 *
 * @author xuqi
 */
public class RescaleTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 这里使用了并行数据源的富函数版本
        // 这样可以调用 getRuntimeContext 方法来获取运行时上下文的一些信息
        env.addSource(new RichParallelSourceFunction<Integer>() {
                    @Override
                    public void run(SourceContext<Integer> sourceContext) {
                        for (int i = 0; i < 8; i++) {
                            // 将奇数发送到索引为 1 的并行子任务
                            // 将偶数发送到索引为 0 的并行子任务
                            if ((i + 1) % 2 == getRuntimeContext().getIndexOfThisSubtask()) {
                                System.out.println((i + 1) + " ---> " + getRuntimeContext().getIndexOfThisSubtask());
                                sourceContext.collect(i + 1);
                            }
                        }
                    }

                    @Override
                    public void cancel() {

                    }
                })
                .setParallelism(2)
                .rescale()
                // .rebalance()
                .print().setParallelism(4);

        env.execute();
    }
}
