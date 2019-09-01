package com.jimi.java.MySQLSource;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author Administrator
 * @create 2019/8/22 14:34
 */

public class FlinkSubmitter {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Student> record = env.addSource(new StudentSourceFromMysql());

        record.print().setParallelism(2);

        env.execute("Flink MySQL Source");
    }
}
