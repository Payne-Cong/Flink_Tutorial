package com.payne.test;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 批处理
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String inPath = "D:\\Dev-Project\\Flink_Tutorial\\src\\main\\resources\\hello.txt";

        DataSource<String> dataSource = env.readTextFile(inPath);

        DataSet<Tuple2<String, Integer>> result= dataSource.flatMap(new myFlatFunctionMapper())
                .groupBy(0) //按照第一个位置的key group by
                .sum(1);// 按照第二个位置 sum

        result.print();


    }

    public static class myFlatFunctionMapper implements FlatMapFunction<String, Tuple2<String,Integer>> {
        /**
         *
         * @param value 输入
         * @param out 输出
         * @throws Exception
         */
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] words = value.split(" ");
            for (String word : words) {
                out.collect(new Tuple2<>(word,1));
            }
        }
    }
}
