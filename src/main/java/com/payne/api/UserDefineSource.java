package com.payne.api;

import com.payne.bean.Student;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;
import java.util.UUID;

public class UserDefineSource implements SourceFunction<Student> {

    private boolean running = true; //循环结束标志位

    @Override
    public void run(SourceContext<Student> ctx) throws Exception {
        while (running){
            ctx.collect(new Student(UUID.randomUUID().toString(),
                    getRandomString(4),
                    new Random().nextInt(100),
                    getClasses()));
        }
        Thread.sleep(5000);
    }

    @Override
    public void cancel() {
        running = false;
    }


    public static String getRandomString(int stringLength) {
        String string = "abcdefghijklmnopqrstuvwxyz";
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < stringLength; i++) {
            int index = (int) Math.floor(Math.random() * string.length());//向下取整0-25
            sb.append(string.charAt(index));
        }
        return sb.toString();
    }

    public static String getClasses(){
        Random random = new Random();
        int i = random.nextInt(10);
        if(i < 3) return "Math";
        else if(i < 6) return "Origin";
        else return "null";
    }

}
