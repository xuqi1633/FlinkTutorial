package org.xq.watermark;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.xq.Event;

import java.util.Calendar;
import java.util.Random;

/**
 * @author xuqi
 */
public class EmitWatermarkInSourceFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new ClickSourceWithWatermark()).print();

        env.execute();
    }

    private static class ClickSourceWithWatermark implements SourceFunction<Event> {
        private boolean running = true;

        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
            Random random = new Random();
            String[] userArr = {"Mary", "Bob", "Alice"};
            String[] urlArr = {"./home", "./cart", "./prod?id=1"};
            while (running) {
                // 毫秒时间戳
                long currTs = Calendar.getInstance().getTimeInMillis();

                String username = userArr[random.nextInt(userArr.length)];
                String url = urlArr[random.nextInt(urlArr.length)];
                Event event = new Event(username, url, currTs);

                // 使用 collectWithTimestamp 方法将数据发送出去，并指明数据中的时间戳的字段
                ctx.collectWithTimestamp(event, currTs);

                // 发送水位线
                ctx.emitWatermark(new Watermark(currTs - 1L));
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
