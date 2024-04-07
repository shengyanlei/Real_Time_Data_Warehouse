package com.shyl.base;

import com.shyl.util.FlinkSourceUtil_kafka;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
public abstract class BaseAPP {
    public abstract void hadnle(StreamExecutionEnvironment env, DataStreamSource<String> stream);

    public void start(int port, int parallelism, String ckAndGroupId, String topic) {
        // TODO: 2024/3/29 1.准备环境

        // TODO: 2024/3/29 1.1 设置hadoop用户
        System.setProperty("HADOOP_USER_NAME", "root");

        // TODO: 2024/3/29 1.2 获取流处理环境，并指定本地测试时启动WebUI所绑定的端口
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port",port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // TODO: 2024/3/29 1.3设置并行度
        env.setParallelism(parallelism);

        // TODO: 2024/3/29 1.4状态后端及检查点的相关配置
            // TODO: 2024/3/29 1.4.1开启状态后端
        env.setStateBackend(new HashMapStateBackend());
            // TODO: 2024/3/29 1.4.2 开始checkpoint
        env.enableCheckpointing(5000);
            // TODO: 2024/3/29 1.4.3 设置checkpoint：精准一次
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
            // TODO: 2024/3/29 1.4.4 checkpoint 存储
//        env.getCheckpointConfig().setCheckpointStorage("");
            // TODO: 2024/3/29 1.4.5 checkpoint 并发数
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
            // TODO: 2024/3/29 1.4.6 checkpoint之间的最小间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
            // TODO: 2024/3/29 1.4.7 checkpoint 超时时间
        env.getCheckpointConfig().setCheckpointTimeout(10000);
            // TODO: 2024/3/29 1.4.8 job取消时 checkpoint 保留策略
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);

        // TODO: 2024/3/29 1.5 从kafka目标主题读取数据，封装成流
        KafkaSource<String> topicDb = FlinkSourceUtil_kafka.getKafkaSource(ckAndGroupId, topic);
        DataStreamSource<String> source = env.fromSource(topicDb, WatermarkStrategy.noWatermarks(), "topic_source");

        // TODO: 2024/3/29  2.执行具体的处理逻辑
        hadnle(env,source);
        // TODO: 2024/3/29  3.job执行
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    };

}
