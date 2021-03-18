package com.payne.test;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 流处理
 */
public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);//并行度

        String inPath = "D:\\Dev-Project\\Flink_Tutorial\\src\\main\\resources\\hello.txt";
        DataStreamSource<String> streamSource = env.readTextFile(inPath);

        DataStream<Tuple2<String, Integer>> streamOperator = streamSource.flatMap(new WordCount.myFlatFunctionMapper())
                .keyBy(0)
                .sum(1);

        streamOperator.print();

        //启动
        env.execute();

    }
}
