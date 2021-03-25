package com.payne.test;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 流处理
 */
public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);//并行度,默认为cpu的核数

        String inPath = "D:\\Dev-Project\\Flink_Tutorial\\src\\main\\resources\\hello.txt";
        DataStreamSource<String> streamSource = env.readTextFile(inPath);

//        ParameterTool fromArgs = ParameterTool.fromArgs(args); //从主函数参数取值 --host localhost --port 7777
//        String host = fromArgs.get("host");
//        int port = fromArgs.getInt("port");
//        env.socketTextStream(host,port);
//        env.socketTextStream("",7777); //nc -lk 7777


        DataStream<Tuple2<String, Integer>> streamOperator = streamSource.flatMap(new WordCount.myFlatFunctionMapper())
                .keyBy(0)
                .sum(1);

        streamOperator.print();

        DataStream<String> singleOutputStreamOperator = streamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] strings = value.split(" ");
                for (String string : strings) {
                    out.collect(string);
                }
            }
        });

        DataStream<Tuple3<String, String, Integer>> outputStreamOperator = singleOutputStreamOperator.map(new MapFunction<String, Tuple3<String, String, Integer>>() {
            @Override
            public Tuple3<String, String, Integer> map(String value) throws Exception {
                return Tuple3.of(value, "随机",1);
            }
        });

        outputStreamOperator.print();

        KeyedStream<Tuple3<String, String, Integer>, String> keyedStream = outputStreamOperator.keyBy(value -> value.f0);
        SingleOutputStreamOperator<Tuple3<String, String, Integer>> sum = keyedStream.sum(2);
        sum.print();


        //启动
        env.execute();

    }
}
