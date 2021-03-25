package com.payne.api;

import com.payne.bean.Student;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.*;

import java.util.Arrays;

public class GroupByApi {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String path = "D:\\Dev-Project\\Flink_Tutorial\\src\\main\\resources\\Student.txt";
        DataSource<String> textFile = env.readTextFile(path);

        MapOperator<String, Student> studentMapOperator = textFile.map(new MapFunction<String, Student>() {
            @Override
            public Student map(String value) throws Exception {
                String[] strings = value.split(" ");
                return Student.of(strings[0], strings[1], Integer.parseInt(strings[2]), strings[3]);
            }
        });




        FilterOperator<Student> studentFilterOperator = studentMapOperator.filter(new FilterFunction<Student>() {
            @Override
            public boolean filter(Student value) throws Exception {
                return value.getClasses().contains("数学");
            }
        });

//        studentFilterOperator.print();


        DataSource<Student> s1 =  env.fromElements(Student.of("1","a",1,"x"),
                Student.of("2","b",2,"x"),
                Student.of("3","c",3,"x"));
        DataSource<Student> s2=  env.fromElements(Student.of("4","d",1,"y"),
                Student.of("5","e",2,"y"),
                Student.of("6","f",3,"y"));
        UnionOperator<Student> unionOperator = s1.union(s2);
        unionOperator.groupBy("classes").reduce(new ReduceFunction<Student>() {
            @Override
            public Student reduce(Student value1, Student value2) throws Exception {
                return Student.of("null",value1.name+"-"+value2.name,value1.source+value2.source,value2.classes);
            }
        }).print();


    }
}
