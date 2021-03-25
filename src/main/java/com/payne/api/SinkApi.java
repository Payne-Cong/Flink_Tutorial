package com.payne.api;

import com.payne.bean.MySQLSink;
import com.payne.bean.Student;
import com.payne.bean.TestDataStream;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisClusterConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Requests;

import javax.annotation.Nullable;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * 数据输出
 */
public class SinkApi {
    public static void main(String[] args) throws Exception {

        DataStream<Student> operator = TestDataStream.getStuDataStream();

        SingleOutputStreamOperator<String> operator1 = operator.map(Student::toString);
        operator1.print();
        //Kafka
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("group.id","consumer-group" );
        properties.put("deserializer.encoding","UTF8");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("auto.offset.reset", "latest");

        class StuSerializationSchema implements KafkaSerializationSchema<String> {
            private String topic;
            StuSerializationSchema(String topic){
                super();
                this.topic = topic;
            }
            @Override
            public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
                return new ProducerRecord<>(topic,element.getBytes(StandardCharsets.UTF_8));
            }
        }


        //官方提供了一些连接器 kafka,cassandra,es,hadoop filesystem,rabbitMQ,NiFi 等
        //Apache Bahir 提供了一些第三方的连接器 Redis,Akka,Netty,ActiveMQ,Flume等

        //输出到kafka里 topic:SinkTests
        //.\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic SinkTests
        DataStreamSink<String> streamSink = operator1.addSink(new FlinkKafkaProducer<String>(
                "SinkTests",
                new StuSerializationSchema("SinkTests"),
                properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        ));


        // redis
        InetSocketAddress socketAddress = InetSocketAddress.createUnresolved("localhost", 6379);
        Set<InetSocketAddress> nodes = new HashSet<>();
        nodes.add(socketAddress);

        FlinkJedisClusterConfig config = new FlinkJedisClusterConfig.Builder()
                .setNodes(nodes)
                .build();

        RedisMapper mapper = new RedisMapper<Student>() {
            // hash StuName:id-sources
            @Override
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.HSET,"StuName");
            }

            @Override
            public String getKeyFromData(Student data) {
                return data.getId();
            }

            @Override
            public String getValueFromData(Student data) {
                return data.getSource().toString();
            }
        };

        operator1.addSink(new RedisSink<>(config, mapper));



        // ES
        List<HttpHost> httpHosts =  new ArrayList<>();
        HttpHost httpHost = new HttpHost("",9200);
        httpHosts.add(httpHost);

        ElasticsearchSinkFunction<Student> elasticsearchSinkFunction = new ElasticsearchSinkFunction<Student>() {
            @Override
            public void process(Student element, RuntimeContext ctx, RequestIndexer indexer) {
                //定义写入的数据
                HashMap<String, String> hashMap = new HashMap<>();
                hashMap.put("id",element.getId());
                hashMap.put("name",element.getName());
                //...省略

                //创建请求
                IndexRequest indexRequest = Requests.indexRequest().index("stu").type("testing").source(hashMap);
                indexer.add(indexRequest); // 发送

            }
        };
        ElasticsearchSink<Student> esSink = new ElasticsearchSink.Builder<>(httpHosts, elasticsearchSinkFunction).build();
        operator.addSink(esSink);



        //mysql
        //自定义连接器
        operator.addSink(new MySQLSink());

        TestDataStream.execute();

    }
}
