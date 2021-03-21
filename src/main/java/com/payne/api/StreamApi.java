package com.payne.api;

import com.payne.bean.Student;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class StreamApi {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(6);//并行度,默认为cpu的核数

        DataStreamSource<Student> studentDataStreamSource = env.fromCollection(Arrays.asList(
                new Student("1", "zhangsan", 98, "math"),
                new Student("2", "lisi", 58, "math"),
                new Student("3", "wangwu", 66, "math"),
                new Student("4", "zhangsan", 86, "origin"),
                new Student("5", "lisi", 62, "origin"),
                new Student("6", "wangwu", 61, "origin")
        ));
        studentDataStreamSource.print("ListSourceDS");

        DataStreamSource<Integer> integerDataStreamSource = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        integerDataStreamSource.print("IntData");


        String path = "D:\\Dev-Project\\Flink_Tutorial\\src\\main\\resources\\Student.txt";
        DataStreamSource<String> textFile = env.readTextFile(path);
        textFile.print();

        // 自定义数据源
        DataStreamSource<Student> userDefineSource = env.addSource(new UserDefineSource());

        userDefineSource.print();


        env.execute("LDSJob");

    }
}
