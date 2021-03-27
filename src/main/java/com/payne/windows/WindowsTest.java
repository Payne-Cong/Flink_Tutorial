package com.payne.windows;

import com.payne.bean.Student;
import com.payne.bean.TestDataStream;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


public class WindowsTest {
    public static void main(String[] args) throws Exception {
        DataStream<Student> stream = TestDataStream.getRandomDataStream();


//        stream.keyBy(Student::getClasses)
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
//        ;
//
//        stream.keyBy(Student::getClasses)
//                .timeWindow(Time.seconds(5)) // 滚动时间
//        ;

        // 注意源码
        //windows function
        //增量聚合函数
        //  每条数据来到就计算,保持一个简单的状态
        //  ReduceFunction 和 AggregationFunction

        //全量聚合函数
        //  先把窗口的数据收集起来,等到要计算的时候 遍历数据
        //  ProcessWindowFunction, WindowFunction

        DataStream<Integer> res = stream.keyBy(Student::getClasses)
                .timeWindow(Time.seconds(3)) // 滚动时间
                .aggregate(new AggregateFunction<Student, Integer, Integer>() {

                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    @Override
                    public Integer add(Student value, Integer accumulator) {
                        return accumulator + 1;
                    }

                    @Override
                    public Integer getResult(Integer accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Integer merge(Integer a, Integer b) {
                        return a+b;
                    }
                });

//        res.print();
        //全量聚合  // IN,OUT,KEY,WINDOWS
        DataStream<Integer> res2 = stream.keyBy(Student::getClasses)
                .timeWindow(Time.seconds(3))
                .apply(new WindowFunction<Student, Integer, String, TimeWindow>() { // IN,OUT,KEY,WINDOWS
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<Student> input, Collector<Integer> out) throws Exception {
                        // s 为统计聚合的key , window 窗口器 , input 输入, out输出
                        Integer size = IteratorUtils.toList(input.iterator()).size();
                        out.collect(size);
                    }
                });

        DataStream<Tuple3<String, Integer,Long>> res3 = stream.keyBy(Student::getClasses)
                .timeWindow(Time.seconds(3))
                .apply(new WindowFunction<Student, Tuple3<String, Integer,Long>, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<Student> input, Collector<Tuple3<String, Integer,Long>> out) throws Exception {
                        long windowStart = window.getStart();
                        long windowEnd = window.getEnd();
                        Integer size = IteratorUtils.toList(input.iterator()).size();
                        out.collect(Tuple3.of(s,size,windowEnd));
                    }
                });

//        res3.print();



        // 计数窗口 求增量平均数
        DataStream<Tuple2<String,Double>> res4 = stream.keyBy(Student::getClasses)
                .countWindow(50, 10) //统计50个开一次窗口 ,每10个滑动一次
                .aggregate(new AggregateFunction<Student, Tuple2<Double, Integer>, Tuple2<String,Double>>() { // 中间结果 存放当前结果 和 当前个数
                    @Override
                    public Tuple2<Double, Integer> createAccumulator() {
                        return Tuple2.of(0.0, 0);
                    }

                    @Override
                    public Tuple2<Double, Integer> add(Student value, Tuple2<Double, Integer> accumulator) {
                        return Tuple2.of(accumulator.f0 + value.getSource(), accumulator.f1 + 1);
                    }

                    @Override
                    public Tuple2<String, Double> getResult(Tuple2<Double, Integer> accumulator) {
                        return Tuple2.of("=====> ",accumulator.f0 / accumulator.f1);
                    }

                    @Override
                    public Tuple2<Double, Integer> merge(Tuple2<Double, Integer> a, Tuple2<Double, Integer> b) {
                        return Tuple2.of(a.f0 + b.f0, b.f1 + b.f1);
                    }
                });

//        res4.print();


        stream.keyBy(Student::getClasses)
                .timeWindow(Time.seconds(3))
                .trigger(new Trigger<Student, TimeWindow>() { // 窗口的计算触发依赖于窗口触发器
                    //触发器的作用就是去控制什么时候来触发我们的聚合方法。Flink内部定义实现了如EventTimeTrigger、ProcessTimeTrigger、CountTrigger以及DeltaTrigger等等
                    //EventTimeTrigger：通过对比Watermark和窗口的Endtime确定是否触发窗口计算，如果Watermark大于Window EndTime则触发，否则不触发，窗口将继续等待。
                    //ProcessTimeTrigger：通过对比ProcessTime和窗口EndTme确定是否触发窗口，如果ProcessTime大于EndTime则触发计算，否则窗口继续等待。
                    //ContinuousEventTimeTrigger：根据间隔时间周期性触发窗口或者Window的结束时间小于当前EndTime触发窗口计算。
                    //ContinuousProcessingTimeTrigger：根据间隔时间周期性触发窗口或者Window的结束时间小于当前ProcessTime触发窗口计算。
                    //CountTrigger：根据接入数据量是否超过设定的阙值判断是否触发窗口计算。
                    //DeltaTrigger：根据接入数据计算出来的Delta指标是否超过指定的Threshold去判断是否触发窗口计算。
                    //PurgingTrigger：可以将任意触发器作为参数转换为Purge类型的触发器，计算完成后数据将被清理。

                    //TriggerResult:
                    //CONTINUE ： 不进行操作，等待。
                    //FIRE ： 触发计算且数据保留。
                    //PRUGE ： 窗口内部数据清除且不触发计算。
                    //FIRE_AND_PURGE : 触发计算并清除对应的数据。
                    @Override
                    public TriggerResult onElement(Student element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
                        //每一个数据进入窗口都会触发。
                        return null;
                    }

                    @Override
                    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                        //根据接入窗口的EventTime进行触发操作
                        return null;
                    }

                    @Override
                    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                        //根据接入窗口的ProcessTime进行触发操作
                        return null;
                    }

                    @Override
                    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
                        //执行窗口及状态数据的清除方法。
                    }
                });

        stream.keyBy(Student::getClasses)
                .timeWindow(Time.seconds(3))
                .evictor(new Evictor<Student, TimeWindow>() { //移除器
                    @Override
                    public void evictBefore(Iterable<TimestampedValue<Student>> elements, int size, TimeWindow window, EvictorContext evictorContext) {

                    }

                    @Override
                    public void evictAfter(Iterable<TimestampedValue<Student>> elements, int size, TimeWindow window, EvictorContext evictorContext) {

                    }
                });

        OutputTag<Student> late = new OutputTag<>("late", TypeInformation.of(Student.class));
        SingleOutputStreamOperator<Student> outputStreamOperator = stream.keyBy(Student::getClasses)
                .timeWindow(Time.seconds(5))
                .allowedLateness(Time.seconds(2)) // 允许迟到的数据
                .sideOutputLateData(late)
                .sum("source");
//        outputStreamOperator.print("all");
//        outputStreamOperator.getSideOutput(late).print("late");


        TestDataStream.execute();

    }
}
