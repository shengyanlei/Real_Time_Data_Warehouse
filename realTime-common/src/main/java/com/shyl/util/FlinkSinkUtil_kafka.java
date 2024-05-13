package com.shyl.util;


import com.shyl.constant.Constant;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;

public class FlinkSinkUtil_kafka {
    public static FlinkKafkaProducer<String> getKafkaSink(String topic) {
        return new FlinkKafkaProducer<>(
                Constant.KAFKA_BROKERS,
                topic,
                new SimpleStringSchema());
    }
    public static void main(String[] args) throws Exception {
        FlinkKafkaProducer<String> test = getKafkaSink("test");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KafkaSource<String> kafkaSource = FlinkSourceUtil_kafka.getKafkaSource("test", "topic_real_db",1);
        DataStreamSource<String> source = env.fromSource(kafkaSource,WatermarkStrategy.noWatermarks(),"kafka-source");
        source.print();
        source.addSink(test);
        env.execute();

    }
}
