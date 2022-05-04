package com.flink.app;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class WindowAPP {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        test01(env);
        env.execute("WindowsAPP");

    }

    public static void test01(StreamExecutionEnvironment env) {
        DataStreamSource<String> source = env.socketTextStream("localhost", 9527);
//        source.map(new MapFunction<String, Integer>() {
//
//            @Override
//            public Integer map(String value) throws Exception {
//
//                return Integer.parseInt(value);
//            }
//        }).windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5))).sum(0).print();

        source.map(new MapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public Tuple2 map(String value) throws Exception {
                String[] splits = value.split(",");

                return Tuple2.of(splits[0].trim(),Integer.parseInt(splits[1]));
            }
        }).keyBy(x ->x.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .sum(1)
                .print();

    }
}

