package com.jimi.java.flink;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.util.Collector;
import scala.Tuple2;

public  class WindowResultFunction implements WindowFunction<Long, Tuple2<String,String>, Tuple, TimeWindow> {

    @Override
    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Long> iterable, Collector<Tuple2<String, String>> collector) throws Exception {


    }
}