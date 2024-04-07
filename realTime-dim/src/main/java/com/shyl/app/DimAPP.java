package com.shyl.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.esotericsoftware.minlog.Log;
import com.shyl.base.BaseAPP;
import com.shyl.constant.Constant;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DimAPP extends BaseAPP {
    public static void main(String[] args) {
        new DimAPP().start(10011,4,"dim_app", Constant.TOPIC_DB);
    }
    @Override
    public void hadnle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        SingleOutputStreamOperator<JSONObject> filterStream = filter(stream);
    }

    private SingleOutputStreamOperator<JSONObject> filter(DataStreamSource<String> stream) {
        SingleOutputStreamOperator<String> filtered = stream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                try {
                    JSONObject jsonObj = JSON.parseObject(s);
                    String db = jsonObj.getString("db");
                    String op = jsonObj.getString("op");
                    String before = jsonObj.getString("before");
                    String after = jsonObj.getString("after");
                    return Constant.MYSQL_DATABASE.equals(db)
                            && ("r".equals(op)) // 读取
                            || ("u".equals(op)) // 更新
                            || ("d".equals(op)) // 删除
                            && before != null
                            || after != null
                            && before.length() >= 2
                            || after.length() >= 2;
                } catch (Exception e) {
                    Log.warn("不是正确的json格式" + s);
                    return false;
                }
            }
        });
        SingleOutputStreamOperator<JSONObject> json = filtered.map(JSON::parseObject);
        return json;
    }
}
