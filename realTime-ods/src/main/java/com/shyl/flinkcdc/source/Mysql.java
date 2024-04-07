package com.shyl.flinkcdc.source;

import com.shyl.constant.Constant;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

public class Mysql {
    public static void main(String[] args) throws Exception {
        MySqlSource<String> mysqlSource = MySqlSource.<String>builder()
                .hostname(Constant.MYSQL_HOST)
                .port(3306)
                .databaseList(Constant.MYSQL_DATABASE)
                .tableList(Constant.TABLE_LIST)  // 表必须有主键
//                .username("flinkcdc")
//                .password("123456")
                .username(Constant.MYSQL_USER_NAME)
                .password(Constant.MYSQL_PASSWORD)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(3000);


        DataStreamSource<String> mysql_source = env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks(), "Mysql Source");
        mysql_source
                .print("11").setParallelism(4);

//      数据导入到卡夫卡中
        mysql_source.addSink(new FlinkKafkaProducer(Constant.KAFKA_BROKERS,Constant.TOPIC_DB,new SimpleStringSchema()));

        env.execute("print Mysql + binlog");


    }
}
