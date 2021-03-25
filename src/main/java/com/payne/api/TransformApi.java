package com.payne.api;

import com.payne.bean.Student;
import com.payne.bean.TestDataStream;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class TransformApi {

    public static void main(String[] args) throws Exception {
        DataStream<Student> operator = TestDataStream.getStuDataStream();

//        DataStream<Student> streamOperator = textFile.map(new MapFunction<String, Student>() {
//            @Override
//            public Student map(String value) throws Exception {
//                String[] strings = value.split(" ");
//                return Student.of(strings[0], strings[1], Integer.valueOf(strings[2]), strings[3]);
//            }
//        });

        DataStream<Student> MathC = operator.filter(value -> value.getClasses().equalsIgnoreCase("数学"));
        MathC.keyBy(Student::getClasses).sum("source");


        KeyedStream<Student, Tuple> keyedStream1 = operator.keyBy("classes");
        keyedStream1.max("source");

        keyedStream1.reduce((v1,v2) ->
                Student.of("","",Math.max(v1.getSource(),v2.getSource()),v2.getClasses()))
                ;


        // 讲一条流切为两条流(相当于打上标签,流本质没变)
//        OutputTag<Student> tag1 = new OutputTag<Student>("数学-FLAG"){};
        OutputTag<Student> tag1 = new OutputTag<Student>("数学-FLAG", TypeInformation.of(Student.class));
        OutputTag<Student> tag2 = new OutputTag<Student>("语文-FLAG"){};
        SingleOutputStreamOperator<Student> process = operator.process(new ProcessFunction<Student, Student>() {
            @Override
            public void processElement(Student value, Context ctx, Collector<Student> out) throws Exception {
                if (value.getClasses().equalsIgnoreCase("数学")) {
                    ctx.output(tag1, value);
                } else {
                    out.collect(value);
                }
            }
        });

        process.print("主流语文");
        process.getSideOutput(tag1).print("测流数学");

        MapFunction<Student, Tuple2<String, Integer>> tuple2MapFunction = new MapFunction<Student, Tuple2<String, Integer>>() {

            @Override
            public Tuple2<String, Integer> map(Student value) throws Exception {
                return Tuple2.of(value.getClasses(), value.getSource());
            }
        };
        SingleOutputStreamOperator<Tuple2<String, Integer>> p1 = process.map(tuple2MapFunction);
        SingleOutputStreamOperator<Tuple2<String, Integer>> p2 = process.getSideOutput(tag1).map(tuple2MapFunction);

        p1.print("p1");
        p2.print("p2");

        // 合流
        ConnectedStreams<Tuple2<String, Integer>, Tuple2<String, Integer>> tuple2ConnectedStreams = p1.connect(p2);


        SingleOutputStreamOperator<Tuple3<String, Integer, String>> res = tuple2ConnectedStreams.map(new CoMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple3<String, Integer, String>>() {
            @Override
            public Tuple3<String, Integer, String> map1(Tuple2<String, Integer> value) throws Exception {
                return Tuple3.of(value.f0, value.f1, "0001");
            }

            @Override
            public Tuple3<String, Integer, String> map2(Tuple2<String, Integer> value) throws Exception {
                return Tuple3.of(value.f0, value.f1, "0002");
            }
            //两条流各自处理

        });

        res.print("res");


        res.shuffle().print("shuffle");

        res.global().print("global"); // 将数据发送到下游的一个分区里去


        TestDataStream.execute();

    }
}
