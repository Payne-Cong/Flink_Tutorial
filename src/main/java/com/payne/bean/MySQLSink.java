package com.payne.bean;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class MySQLSink extends RichSinkFunction<Student> {

    Connection connection = null;
    PreparedStatement insertStatement = null;
    PreparedStatement updateStatement = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = DriverManager.getConnection("jdbc:mysql://localhost:3306", "root", "root");
        insertStatement=connection.prepareCall("insert into table_name(id , name ) values (?,?)");
        updateStatement=connection.prepareCall("update table_name set id = ? where name = ?");
    }

    //每来一条数据执行
    @Override
    public void invoke(Student value, Context context) throws Exception {
        updateStatement.setString(1,value.getId());
        updateStatement.setString(2,value.getName());
        updateStatement.execute();
        if(updateStatement.getUpdateCount() == 0){ // 没有更新成功就插入
            insertStatement.setString(1,value.getId());
            insertStatement.setString(2,value.getName());
            insertStatement.execute();
        }
    }

    @Override
    public void close() throws Exception {
        updateStatement.close();
        insertStatement.close();
        connection.close();
    }
}
