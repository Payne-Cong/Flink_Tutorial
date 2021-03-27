package com.payne.bean;

import com.payne.api.UserDefineSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TestDataStream {
    static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    //并行度,默认为cpu的核数
    public static DataStream<Student> getStuDataStream(){


        String path = "D:\\Dev-Project\\Flink_Tutorial\\src\\main\\resources\\Student.txt";
        DataStreamSource<String> textFile = env.readTextFile(path);

        DataStream<Student> operator = textFile.map(value -> {
            String[] strings = value.split(" ");
            return Student.of(strings[0], strings[1], Integer.valueOf(strings[2]), strings[3]);
        });
        return operator;
    }

    public static DataStream<Student> getRandomDataStream(){
        DataStreamSource<Student> userDefineSource = env.addSource(new UserDefineSource());
        return  userDefineSource;
    }

    public static void execute() throws Exception {
        env.execute();
    }
}
