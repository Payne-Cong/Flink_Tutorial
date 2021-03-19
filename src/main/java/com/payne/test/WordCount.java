package com.payne.test;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.MapOperator;
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

        FlatMapOperator<String, String> colData = dataSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] strings = value.split(" ");
                for (String string : strings) {
                    out.collect(string);
                }
            }
        });

        // 先转换成单列,在map转(k,k.length)
        MapOperator<String, Tuple2<String, Integer>> mapOperator = colData.map(new MapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return new Tuple2<>(value,value.length());
            }
        });

        mapOperator.print();

        //过滤
        colData.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return !value.contains("hello");
            }
        }).print();


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
