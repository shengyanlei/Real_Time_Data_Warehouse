package com.shyl.flinkcdc.source;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.table.planner.expressions.TableReference;
import org.apache.kafka.clients.producer.KafkaProducer;

public class Mysql {
    public static void main(String[] args) throws Exception {
        MySqlSource<String> mysqlSource = MySqlSource.<String>builder()
                .hostname("192.168.52.201")
                .port(3306)
                .databaseList("flinkcdc")
                .tableList("flinkcdc.activity_info")
//                .username("flinkcdc")
//                .password("123456")
                .username("root")
                .password("1234kxmall!@#ABC")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(3000);


        DataStreamSource<String> mysql_source = env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks(), "Mysql Source");
        mysql_source
                .print("11").setParallelism(1);
//      数据导入到卡夫卡中
        mysql_source.addSink(new FlinkKafkaProducer("192.168.52.201:9092","topic_flinksql",new SimpleStringSchema()));

        env.execute("print Mysql + binlog");


    }
}
