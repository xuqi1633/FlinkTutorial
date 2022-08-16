package org.xq.source;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Random;

/**
 * @author xuqi
 */
public class ParallelSourceExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(new CustomSource()).setParallelism(2).print().setParallelism(2);

        env.execute();
    }

    /**
     * 自定义并行的数据源
     */
    private static class CustomSource implements ParallelSourceFunction<Integer> {
        private boolean running = true;
        private final Random random = new Random();

        @Override
        public void run(SourceContext<Integer> sourceContext) throws Exception {
            while (running) {
                sourceContext.collect(random.nextInt(100));
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
