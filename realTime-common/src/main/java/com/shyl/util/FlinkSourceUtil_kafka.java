package com.shyl.util;

import com.shyl.constant.Constant;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;



//KAFKA作为flink 的ods层数据，即flink程序中的source
public class FlinkSourceUtil_kafka {
    public static KafkaSource <String> getKafkaSource(String groupId, String topic){
        return KafkaSource.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setGroupId(groupId)
                .setTopics(topic)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new DeserializationSchema<String>() {

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return Types.STRING;
                    }

                    @Override
                    public String deserialize(byte[] bytes) throws IOException {
                        if (bytes != null) {
                            return new String(bytes, StandardCharsets.UTF_8);
                        }
                        return null;
                    }

                    @Override
                    public boolean isEndOfStream(String s) {
                        return false;
                    }
                })
                .build();
    }

}
