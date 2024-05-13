package com.shyl.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.esotericsoftware.minlog.Log;
import com.shyl.base.BaseAPP;
import com.shyl.bean.TableProcessDim;
import com.shyl.constant.Constant;
import com.shyl.util.HbaseUtil;
import com.shyl.util.JDBCUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.*;


import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import static com.shyl.util.HbaseUtil.*;


public class DimAPP extends BaseAPP {

    public static void main(String[] args) {
        new DimAPP().start(10011, 4, "dim_app", Constant.TOPIC_DB);
    }
    @Override
    public void hadnle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // TODO: 2024/4/8 对原始数据进行清洗
        SingleOutputStreamOperator<JSONObject> filterStream = filter(stream);
        // TODO: 2024/4/8 使用flink-cdc读取监控配置表数据
        SingleOutputStreamOperator<TableProcessDim> tableProcessDimSingleOutputStreamOperator = readTableProcess(env);
        // TODO: 2024/4/8 在Hbase创建维度表
        SingleOutputStreamOperator<TableProcessDim> hbaseTable = createHbaseTable(tableProcessDimSingleOutputStreamOperator);
        // TODO: 2024/4/23 将主流和配置流进行关联，并对关联后的数据流进行处理
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> connect = connect(filterStream, hbaseTable);
        // TODO: 2024/4/25 将维度表的数据写入到hbase中
//        connect.print();
        witeToHbase(connect);
    }

    private void witeToHbase(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> connect) {
        connect.print("data:");
        connect
//                .keyBy(t -> t.f1.getSinkTable())
                .map(new map()).print();
    }
    private class map implements Serializable, MapFunction<Tuple2<JSONObject, TableProcessDim>, Tuple2<JSONObject, TableProcessDim>> {
            @Override
            public Tuple2<JSONObject, TableProcessDim> map(Tuple2<JSONObject, TableProcessDim> value) throws Exception {
                String HbaseTable = value.f1.getSinkTable();
                System.out.println(HbaseTable);
                String sinkFamily = value.f1.getSinkFamily();
                String[] keyColumns = value.f1.getSinkColumns().split(",");
                String sinkRowKey = value.f0.getString(value.f1.getSinkRowKey());
                if (value.f0.getString("op").equals("r") || value.f0.getString("op").equals("c")){
                    insertHbaseData(Constant.HBASE_NAMESPACE,HbaseTable,sinkFamily,sinkRowKey,keyColumns,value.f0);
                }else if(value.f0.getString("op").equals("u")){
                    deleteHbaseData(Constant.HBASE_NAMESPACE,HbaseTable,sinkRowKey);
                    insertHbaseData(Constant.HBASE_NAMESPACE,HbaseTable,sinkFamily,sinkRowKey,keyColumns,value.f0);
                }else{
                    deleteHbaseData(Constant.HBASE_NAMESPACE,HbaseTable,sinkRowKey);
                }

                return value;
            }
        }

    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> connect(SingleOutputStreamOperator<JSONObject> filterStream, SingleOutputStreamOperator<TableProcessDim> hbaseTable) {
        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor = new MapStateDescriptor<>("table-process-dim", String.class, TableProcessDim.class);
//        1.将配置流转换为广播流
        BroadcastStream<TableProcessDim> broadcast = hbaseTable.broadcast(mapStateDescriptor);
//        2.将主流和广播流进行连接
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> tuple2SingleOutputStreamOperator = filterStream.connect(broadcast)
                .process(new BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>() {
                    private HashMap<String, TableProcessDim> map;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        map = new HashMap<>();
                        java.sql.Connection mysql = JDBCUtil.getMysqlConnection();
                        List<TableProcessDim> tableProcessDims = JDBCUtil.queryList(mysql, "select * from gmall2024_config.table_process_dim", TableProcessDim.class, true);
                        for (TableProcessDim tableProcessDim : tableProcessDims){
                            map.put(tableProcessDim.getSourceTable(), tableProcessDim);
                        }
                        JDBCUtil.closeConnection(mysql);
                    }

                    @Override
                    public void processElement(JSONObject value, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.ReadOnlyContext ctx, Collector<Tuple2<JSONObject, TableProcessDim>> out) throws Exception {
//                        1.获取只读的广播状态
                        ReadOnlyBroadcastState<String, TableProcessDim> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                        String tableName = value.getJSONObject("source").getString("table");
                        TableProcessDim tableProcessDim = broadcastState.get(tableName);
                        JSONObject data;
                        if (tableProcessDim == null) {
                            tableProcessDim = map.get(tableName);
                            if (tableProcessDim != null) {
                                Log.info("在map中查找到key" + tableProcessDim);
                            }
                        } else {
                            Log.info("在状态中查找到key" + tableProcessDim);
//                            finnk-cdc 处理后的数据，分为crud四种类型，如果类型是d，则取before，如果类型是c、r、u，则取after
                            }
                        if (tableProcessDim != null){
                            if ("d".equals(value.getString("op"))) {
                                data = value.getJSONObject("before");
                            } else {
                                data = value.getJSONObject("after");
                            }
                            data.put("op", value.getString("op"));
                            out.collect(Tuple2.of(data, tableProcessDim));
                        }
                    }

                    @Override
                    public void processBroadcastElement(TableProcessDim value, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.Context ctx, Collector<Tuple2<JSONObject, TableProcessDim>> out) throws Exception {
//                    分析一下这个方法应该执行的逻辑
                        // 1.获取广播流中的数据
                        BroadcastState<String, TableProcessDim> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                        String key = value.getSourceTable();
                        // 2.判断是否为空
                        if ("d".equals(value.getOp())) {
                            // 3.如果为空，则删除广播流中的数据
                            broadcastState.remove(key);
                        } else {
                            // 4.如果不为空，则更新广播流中的数据
                            broadcastState.put(key, value);
                        }
                    }

                }).setParallelism(1);
        return tuple2SingleOutputStreamOperator;

    }


    private SingleOutputStreamOperator<JSONObject> filter(DataStreamSource<String> stream) {
        SingleOutputStreamOperator<String> filtered = stream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                try {
                    JSONObject jsonObj = JSON.parseObject(s);
                    String db = jsonObj.getJSONObject("source").getString("db");
                    String op = jsonObj.getString("op");
                    System.out.println(op);
                    String before = jsonObj.getString("before");
                    String after = jsonObj.getString("after");
                    return Constant.MYSQL_DATABASE.equals(db)
                            && ("r".equals(op)) // 读取
                            || ("u".equals(op)) // 更新
                            || ("d".equals(op)) // 删除
                            || ("c".equals(op)) // 创建
                            && before != null
                            || after != null;
                } catch (Exception e) {
                    Log.warn("不是正确的json格式" + s);
                    return false;
                }
            }
        });
        SingleOutputStreamOperator<JSONObject> json = filtered.map(JSON::parseObject);
        return json;
    }

    private SingleOutputStreamOperator<TableProcessDim> readTableProcess(StreamExecutionEnvironment env) {
        Properties properties = new Properties();
        properties.setProperty("useSSL", "false");
        properties.setProperty("allowPublicKeyRetrieval", "true");
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(Constant.MYSQL_HOST)
                .port(Constant.MYSQL_PORT)
                .databaseList(Constant.MYSQL2HBASE_CONFIG_DATABASE)
                .tableList(Constant.MYSQL2HBASE_CONFIG_DATABASE.concat(".table_process_dim"))
                .username(Constant.MYSQL_USER_NAME)
                .password(Constant.MYSQL_PASSWORD)
                .jdbcProperties(properties)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();

        return env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "cdc-source")
                .map(new MapFunction<String, TableProcessDim>() {
                    @Override
                    public TableProcessDim map(String s) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(s);
                        String op = jsonObject.getString("op");
                        TableProcessDim tableProcessDim;
                        if ("d".equals(op)) {
                            tableProcessDim = jsonObject.getObject("before", TableProcessDim.class);
                        } else {
                            tableProcessDim = jsonObject.getObject("after", TableProcessDim.class);
                        }
                        tableProcessDim.setOp(op);
                        return tableProcessDim;
                    }
                }).setParallelism(1);
    }

    private SingleOutputStreamOperator<TableProcessDim> createHbaseTable(SingleOutputStreamOperator<TableProcessDim> tpstream){
        return tpstream.map(new RichMapFunction<TableProcessDim, TableProcessDim>() {
            private Connection hbaseconn;

            @Override
            public void open(Configuration parameters) throws Exception {
                hbaseconn = HbaseUtil.getHbaseConnection();
            }

            @Override
            public void close() throws Exception {
                HbaseUtil.closeHbaseConnection(hbaseconn);
            }

            @Override
            public TableProcessDim map(TableProcessDim tableProcessDim) throws Exception {
                String op = tableProcessDim.getOp();
                if ("d".equals(op)){
                    dropTable(tableProcessDim);
                } else if ("r".equals(op) || "c".equals(op)) {
                    createTable(tableProcessDim);
                }else {
                    dropTable(tableProcessDim);
                    createTable(tableProcessDim);
                }
                return tableProcessDim;
            }
            private void createTable(TableProcessDim tableProcessDim) throws IOException {
                HbaseUtil.createHbaseTable(hbaseconn,Constant.HBASE_NAMESPACE,tableProcessDim.getSinkTable(),tableProcessDim.getSinkFamily());
            }
            private void dropTable(TableProcessDim tableProcessDim) throws IOException {
                HbaseUtil.dropHBaseTable(hbaseconn,Constant.HBASE_NAMESPACE,tableProcessDim.getSinkTable());
            }

        }).setParallelism(1);

    }

}
