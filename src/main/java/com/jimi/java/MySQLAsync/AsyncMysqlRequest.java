package com.jimi.java.MySQLAsync;

import jdk.nashorn.internal.ir.ObjectNode;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;


/**
 * @Author Administrator
 * @create 2019/8/26 15:31
 */
public class AsyncMysqlRequest {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","master126:9092");
        properties.setProperty("zookeeper.connect","master126:2181");
        properties.setProperty("group.id", "cloudera_mirrormaker");

        String topic = "flink-topic";

        FlinkKafkaConsumer<ObjectNode> source = new FlinkKafkaConsumer(topic, new JsonNodeDeserializationSchema(), properties);

        /*env.addSource(source).map(value -> {

            String imei = value.get("imei").asText();

            return new UserRelation();
        });*/
    }

}
