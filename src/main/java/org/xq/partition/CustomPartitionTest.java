package org.xq.partition;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author xuqi
 */
public class CustomPartitionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 将自然数按照奇偶分区
        // 先经过 key 选择器 选择出key  再经过分区器进行分区
        env.fromElements(1, 2, 3, 4, 5, 6, 7, 8)
                .partitionCustom(new Partitioner<Integer>() {
                    /**
                     * @param key The key.
                     * @param numPartitions The number of partitions to partition into.
                     * @return The partition index.
                     */
                    @Override
                    public int partition(Integer key, int numPartitions) {
                        return key % numPartitions;
                    }
                }, new KeySelector<Integer, Integer>() {
                    /**
                     *
                     * @param value  The object to get the key from.
                     * @return The extracted key
                     */
                    @Override
                    public Integer getKey(Integer value) {
                        return value;
                    }
                })
                .print().setParallelism(3);


        env.execute();
    }
}
