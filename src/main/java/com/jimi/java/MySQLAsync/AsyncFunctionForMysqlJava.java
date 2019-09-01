package com.jimi.java.MySQLAsync;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import org.apache.flink.configuration.Configuration;

/**
 * @Author Administrator
 * @create 2019/8/26 18:16
 */

public class AsyncFunctionForMysqlJava extends RichAsyncFunction<UserRelation ,UserRelation> {

    // Log
    Logger logger = LoggerFactory.getLogger(AsyncFunctionForMysqlJava.class);
    // trainsient 不参与序列化
    private transient MySqlClient client;
    private transient ExecutorService executorService;

    /*
     * open方法中初始化
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        logger.info("async function for mysql java open ...");
        super.open(parameters);

        client = new MySqlClient();
        executorService = Executors.newFixedThreadPool(30);

    }

    @Override
    public void asyncInvoke(UserRelation ur, ResultFuture<UserRelation> resultFuture)  {

        executorService.submit(()-> {
            // submit query
            System.out.println("submit query" + ur.getImei() + "-1-" + System.currentTimeMillis());
            UserRelation tmp = client.query1(ur);
            resultFuture.complete(Collections.singletonList(tmp));
                });
    }

    @Override
    public void timeout (UserRelation input, ResultFuture<UserRelation> resultFuture)  {
        logger.warn("Async function for hbase timeout");
        List<UserRelation> list = new ArrayList<>();
        input.setImei("timeout");
        list.add(input);
        resultFuture.complete(list);
    }

    @Override
    public void close() throws Exception{
        logger.info("async function for mysql java close");
        super.close();
    }
}
