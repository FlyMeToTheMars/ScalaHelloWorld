package com.jimi.java.flink;

import scala.Tuple2;
import org.apache.flink.api.common.functions.AggregateFunction;

public class CountAgg implements AggregateFunction<Tuple2<String,String>, Long, Long>{
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(Tuple2<String,String> key, Long acc) {
        return acc + 1;
    }

    @Override
    public Long getResult(Long acc) {
        return acc;
    }

    @Override
    public Long merge(Long acc1, Long acc2) {
        return acc1 + acc2;
    }
}
