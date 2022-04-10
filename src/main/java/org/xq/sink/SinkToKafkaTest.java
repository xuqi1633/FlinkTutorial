package org.xq.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * @author xuqi
 */
public class SinkToKafkaTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "test112:9092");

        DataStreamSource<String> stream = env.readTextFile("input/clicks.csv");

        stream.addSink(new FlinkKafkaProducer<>(
                "clicks", new SimpleStringSchema(), properties));

        env.execute();
    }
}
