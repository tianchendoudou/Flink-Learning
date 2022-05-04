package com.imooc.flink.tansformation;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformationApp {
    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //map(env);

        filter(env);

        env.execute("Tansformation");

    }

    public static void filter(StreamExecutionEnvironment env){
        DataStreamSource<String> source = env.readTextFile("data/access.log");
        SingleOutputStreamOperator<Access> mapStream = source.map(new MapFunction<String, Access>() {
            @Override
            public Access map(String value) throws Exception {
                String[] splits = value.split(",");
                Long time = Long.parseLong(splits[0].trim());
                String domain = splits[1].trim();
                Double traffic = Double.parseDouble(splits[2].trim());


                return new Access(time, domain, traffic);
            }
        });

        SingleOutputStreamOperator<Access> filterStream = mapStream.filter(new FilterFunction<Access>() {
            @Override
            public boolean filter(Access value) throws Exception {


                return value.getTraffic() > 20000;
            }
        });

        filterStream.print();



    };

    public static void map(StreamExecutionEnvironment env){
        DataStreamSource<String> source = env.readTextFile("data/access.log");
        SingleOutputStreamOperator<Access> mapStream = source.map(new MapFunction<String, Access>() {
            @Override
            public Access map(String value) throws Exception {
                String[] splits = value.split(",");
                Long time = Long.parseLong(splits[0].trim());
                String domain = splits[1].trim();
                Double traffic = Double.parseDouble(splits[2].trim());


                return new Access(time, domain, traffic);
            }
        });

        mapStream.print();

    }
}
