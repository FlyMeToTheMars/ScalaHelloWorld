package com.jimi.java.kafkademo;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

//使用并行度为1的source
public class MyNoParalleSource implements SourceFunction<String> {//1

    //private long count = 1L;
    private boolean isRunning = true;

    /**
     * 主要的方法
     * 启动一个source
     * 大部分情况下，都需要在这个run方法中实现一个循环，这样就可以循环产生数据了
     *
     * @param ctx
     * @throws Exception
     */
    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while(isRunning){

            List<String> books = new ArrayList<>();
            // MySQL 国外imei选取
            books.add("{\"accStatus\":\"NULL\",\"addr\":\"\",\"alertType\":\"3\",\"fenceId\":\"NULL\",\"gpsTime\":\"NULL\",\"iccid\":\"NULL\",\"imei\":\"000000000000003\",\"imsi\":\"NULL\",\"lat\":\"39.868658\",\"lng\":\"116.454836\",\"offlineTime\":\"NULL\",\"postTime\":\"2019-01-30 20:32:33\",\"time\":\"NULL\",\"type\":\"DEVICE\"}");//10
            books.add("{\"accStatus\":\"NULL\",\"addr\":\"\",\"alertType\":\"3\",\"fenceId\":\"NULL\",\"gpsTime\":\"NULL\",\"iccid\":\"NULL\",\"imei\":\"000000000000000\",\"imsi\":\"NULL\",\"lat\":\"39.868658\",\"lng\":\"116.454836\",\"offlineTime\":\"NULL\",\"postTime\":\"2019-01-30 20:32:33\",\"time\":\"NULL\",\"type\":\"DEVICE\"}\n");//8
            books.add("{\"accStatus\":\"NULL\",\"addr\":\"\",\"alertType\":\"3\",\"fenceId\":\"NULL\",\"gpsTime\":\"NULL\",\"iccid\":\"NULL\",\"imei\":\"000000000000256\",\"imsi\":\"NULL\",\"lat\":\"39.868658\",\"lng\":\"116.454836\",\"offlineTime\":\"NULL\",\"postTime\":\"2019-01-30 20:32:33\",\"time\":\"NULL\",\"type\":\"DEVICE\"}");//5
            books.add("{\"accStatus\":\"NULL\",\"addr\":\"\",\"alertType\":\"3\",\"fenceId\":\"NULL\",\"gpsTime\":\"NULL\",\"iccid\":\"NULL\",\"imei\":\"000000000000361\",\"imsi\":\"NULL\",\"lat\":\"39.868658\",\"lng\":\"116.454836\",\"offlineTime\":\"NULL\",\"postTime\":\"2019-01-30 20:32:33\",\"time\":\"NULL\",\"type\":\"DEVICE\"}\n");//3
            books.add("{\"accStatus\":\"NULL\",\"addr\":\"\",\"alertType\":\"3\",\"fenceId\":\"NULL\",\"gpsTime\":\"NULL\",\"iccid\":\"NULL\",\"imei\":\"000000000000362\",\"imsi\":\"NULL\",\"lat\":\"39.868658\",\"lng\":\"116.454836\",\"offlineTime\":\"NULL\",\"postTime\":\"2019-01-30 20:32:33\",\"time\":\"NULL\",\"type\":\"DEVICE\"}");//0-4
            books.add("{\"accStatus\":\"NULL\",\"addr\":\"\",\"alertType\":\"3\",\"fenceId\":\"NULL\",\"gpsTime\":\"NULL\",\"iccid\":\"NULL\",\"imei\":\"000000000000620\",\"imsi\":\"NULL\",\"lat\":\"39.868658\",\"lng\":\"116.454836\",\"offlineTime\":\"NULL\",\"postTime\":\"2019-01-30 20:32:33\",\"time\":\"NULL\",\"type\":\"DEVICE\"}\n");

            /*books.add("{\"accStatus\":\"NULL\",\"addr\":\"\",\"alertType\":\"3\",\"fenceId\":\"NULL\",\"gpsTime\":\"NULL\",\"iccid\":\"NULL\",\"imei\":\"201711071236521\",\"imsi\":\"NULL\",\"lat\":\"39.868658\",\"lng\":\"116.454836\",\"offlineTime\":\"NULL\",\"postTime\":\"2019-01-30 20:32:33\",\"time\":\"NULL\",\"type\":\"DEVICE\"}\n");
            books.add("{\"accStatus\":\"NULL\",\"addr\":\"\",\"alertType\":\"3\",\"fenceId\":\"NULL\",\"gpsTime\":\"NULL\",\"iccid\":\"NULL\",\"imei\":\"201711071236523\",\"imsi\":\"NULL\",\"lat\":\"39.868658\",\"lng\":\"116.454836\",\"offlineTime\":\"NULL\",\"postTime\":\"2019-01-30 20:32:33\",\"time\":\"NULL\",\"type\":\"DEVICE\"}\n");
            books.add("{\"accStatus\":\"NULL\",\"addr\":\"\",\"alertType\":\"3\",\"fenceId\":\"NULL\",\"gpsTime\":\"NULL\",\"iccid\":\"NULL\",\"imei\":\"358740055834401\",\"imsi\":\"NULL\",\"lat\":\"39.868658\",\"lng\":\"116.454836\",\"offlineTime\":\"NULL\",\"postTime\":\"2019-01-30 20:32:33\",\"time\":\"NULL\",\"type\":\"DEVICE\"}\n");
            books.add("{\"accStatus\":\"NULL\",\"addr\":\"\",\"alertType\":\"3\",\"fenceId\":\"NULL\",\"gpsTime\":\"NULL\",\"iccid\":\"NULL\",\"imei\":\"201711078888880\",\"imsi\":\"NULL\",\"lat\":\"39.868658\",\"lng\":\"116.454836\",\"offlineTime\":\"NULL\",\"postTime\":\"2019-01-30 20:32:33\",\"time\":\"NULL\",\"type\":\"DEVICE\"}\n");
            books.add("{\"accStatus\":\"NULL\",\"addr\":\"\",\"alertType\":\"3\",\"fenceId\":\"NULL\",\"gpsTime\":\"NULL\",\"iccid\":\"NULL\",\"imei\":\"201711078221117\",\"imsi\":\"NULL\",\"lat\":\"39.868658\",\"lng\":\"116.454836\",\"offlineTime\":\"NULL\",\"postTime\":\"2019-01-30 20:32:33\",\"time\":\"NULL\",\"type\":\"DEVICE\"}\n");
            books.add("{\"accStatus\":\"NULL\",\"addr\":\"\",\"alertType\":\"3\",\"fenceId\":\"NULL\",\"gpsTime\":\"NULL\",\"iccid\":\"NULL\",\"imei\":\"201711078888888\",\"imsi\":\"NULL\",\"lat\":\"39.868658\",\"lng\":\"116.454836\",\"offlineTime\":\"NULL\",\"postTime\":\"2019-01-30 20:32:33\",\"time\":\"NULL\",\"type\":\"DEVICE\"}\n");
            books.add("{\"accStatus\":\"NULL\",\"addr\":\"\",\"alertType\":\"3\",\"fenceId\":\"NULL\",\"gpsTime\":\"NULL\",\"iccid\":\"NULL\",\"imei\":\"201711070128877\",\"imsi\":\"NULL\",\"lat\":\"39.868658\",\"lng\":\"116.454836\",\"offlineTime\":\"NULL\",\"postTime\":\"2019-01-30 20:32:33\",\"time\":\"NULL\",\"type\":\"DEVICE\"}\n");*/

            int i = new Random().nextInt(5);
            ctx.collect(books.get(i));

            //每2秒产生一条数据
            Thread.sleep(20);
        }
    }

    //取消一个cancel的时候会调用的方法
    @Override
    public void cancel() {
        isRunning = false;
    }

}

