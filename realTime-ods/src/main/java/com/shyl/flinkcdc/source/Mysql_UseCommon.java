package com.shyl.flinkcdc.source;

import com.shyl.base.BaseAPP;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class Mysql_UseCommon extends BaseAPP {
    public static void main(String[] args) {
    BaseAPP mysqlUseCommon = new Mysql_UseCommon();
    mysqlUseCommon.start(10001,4,"test","topic_real_db");
    }

    @Override
    public void hadnle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        stream.print();
    }

}
