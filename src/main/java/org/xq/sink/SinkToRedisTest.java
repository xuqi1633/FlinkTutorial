package org.xq.sink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.xq.Event;
import org.xq.source.ClickSource;

/**
 * @author xuqi
 */
public class SinkToRedisTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 创建一个到 redis 连接的配置
        FlinkJedisPoolConfig conf = new
                FlinkJedisPoolConfig.Builder().setHost("hadoop102").build();
        env.addSource(new ClickSource())
                .addSink(new RedisSink<Event>(conf, new MyRedisMapper()));
        env.execute();
    }

    public static class MyRedisMapper implements RedisMapper<Event> {
        @Override
        public String getKeyFromData(Event e) {
            return e.user;
        }

        @Override
        public String getValueFromData(Event e) {
            return e.url;
        }

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "clicks");
        }
    }
}
