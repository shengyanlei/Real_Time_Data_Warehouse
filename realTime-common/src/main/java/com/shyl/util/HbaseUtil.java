package com.shyl.util;


import com.alibaba.fastjson.JSONObject;
import com.esotericsoftware.minlog.Log;
import com.google.gson.JsonObject;
import com.shyl.constant.Constant;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;


import java.io.IOException;

@Slf4j
public class HbaseUtil {

//    todo：获取hbase连接
    public static Connection getHbaseConnection() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("hbase.zookeeper.quorum", "nodev2001");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        return ConnectionFactory.createConnection(configuration);
    }
//  todo :关闭hbase连接
    public static void closeHbaseConnection(Connection hbaseconn) throws IOException {
        if (hbaseconn != null && !hbaseconn.isClosed()){
            hbaseconn.close();
        }
    }
//  todo：  创建hbase表
    public static void createHbaseTable(Connection connection,String namespace,String table,String family) throws IOException {
        Admin admin = connection.getAdmin();
        TableName tableName = TableName.valueOf(namespace,table);
//        判断表是否存在
        if (admin.tableExists(tableName)){
            log.info(namespace+"下"+table+"已存在");
            return;
        }
//        列族描述器
        ColumnFamilyDescriptor cfDesc = ColumnFamilyDescriptorBuilder.of(family);
//        表描述器
        TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
                .setColumnFamily(cfDesc)
                .build();
        admin.createTable(tableDescriptor);
        admin.close();
        log.info(namespace+" "+table+"建表成功");
    }
//   todo： 删除hbase指定的表
    public static void dropHBaseTable(Connection hbaseConn,
                                      String nameSpace,
                                      String table) throws IOException {

        Admin admin = hbaseConn.getAdmin();
        TableName tableName = TableName.valueOf(nameSpace, table);
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            log.info(nameSpace + " " + table + " 删除成功");
        }else{
            log.info(nameSpace + "下" + table + "不存在");
        }
        admin.close();

    }
    // TODO: 2024/5/9 向hbase表中插入数据
    public static void insertHbaseData(String nameSpace,
                                     String table,
                                     String family,
                                     String rowKey,
                                     String[] columns,
                                     JSONObject value) {
        Connection hbaseConnection = HbaseConnectionPool.getConnection();
        TableName tableName = TableName.valueOf(nameSpace, table);
        Table hbaseTable = null;
        try {
            hbaseTable = hbaseConnection.getTable(tableName);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Put put = new Put(rowKey.getBytes());
        for (String column : columns){
            if (value.getString(column) != null) {
                put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), value.getString(column).getBytes());
            }
        }
        try {
            hbaseTable.put(put);
            Log.info(hbaseTable+"插入数据成功：rowkey："+rowKey);
            HbaseConnectionPool.retrunConnection(hbaseConnection);
        } catch (IOException e) {
            Log.info(hbaseTable+"插入数据失败：rowkey："+rowKey);
            HbaseConnectionPool.retrunConnection(hbaseConnection);
            throw new RuntimeException(e);
        }
    }

    // TODO: 2024/5/9 删除hbase表指定rowkey数据，指定column就删除该rowkey下指定column的数据，否则就将该rowkey全部删除
    public static void deleteHbaseData(String nameSpace,
                                       String table,
                                       String rowKey,
                                       String ... column) {
        Connection hbaseConnection = HbaseConnectionPool.getConnection();
        TableName tableName = TableName.valueOf(nameSpace, table);
        Table hbaseTable = null;
        try {
            hbaseTable = hbaseConnection.getTable(tableName);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Delete delete = new Delete(rowKey.getBytes());
        for (String s : column) {
            delete.addColumn(Bytes.toBytes("info"),Bytes.toBytes(s));
        }
        try {
            hbaseTable.delete(delete);
            hbaseTable.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        HbaseConnectionPool.retrunConnection(hbaseConnection);
    }
//    创建一个指定rowkey读取hbase表数据的方法
    public static Result getHbaseData(
                                      String nameSpace,
                                      String table,
                                      String rowKey,
                                      String ... column) throws IOException {
        Connection hbaseConnection = HbaseUtil.getHbaseConnection();
        TableName tableName = TableName.valueOf(nameSpace, table);
        Table hbaseTable = hbaseConnection.getTable(tableName);
        Get get = new Get(rowKey.getBytes());
        if (column != null){
            for (String col : column){
                get.addColumn(Bytes.toBytes("info"),Bytes.toBytes(col));
            }
        }
        Result result = hbaseTable.get(get);
        hbaseTable.close();
        HbaseUtil.closeHbaseConnection(hbaseConnection);
        return result;
    }

//    统计某个hbase表的数据量
    public static int countHbaseData(String nameSpace,String table) throws IOException {
        Connection hbaseConnection = HbaseUtil.getHbaseConnection();
        TableName tableName = TableName.valueOf(nameSpace, table);
        Table hbaseTable = hbaseConnection.getTable(tableName);
        Scan scan = new Scan();
        ResultScanner scanner = hbaseTable.getScanner(scan);
        int count = 0;
        for (Result result : scanner){
            count++;
        }
        log.info(nameSpace+" "+table+"数据量"+count);
        hbaseTable.close();
        HbaseUtil.closeHbaseConnection(hbaseConnection);
        return count;
    }

//    查询hbase表的数据
    public static void queryHbaseData(String nameSpace,String table,String rowKey) throws IOException {
        Connection hbaseConnection = HbaseUtil.getHbaseConnection();
        TableName tableName = TableName.valueOf(nameSpace, table);
        Table hbaseTable = hbaseConnection.getTable(tableName);
        Get get = new Get(rowKey.getBytes());
        Result result = hbaseTable.get(get);
        for (Cell cell : result.rawCells()){
            System.out.println(Bytes.toString(cell.getRowArray(),cell.getRowOffset(),cell.getRowLength())+"--"+
                    Bytes.toString(cell.getFamilyArray(),cell.getFamilyOffset(),cell.getFamilyLength())+"--"+
                    Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength())+"--"+
                    Bytes.toString(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength()));
                }
            }

    public static void main(String[] args) {
        String[] tableNames = {
                "dim_activity_info",
                "dim_activity_rule",
                "dim_activity_sku",
                "dim_base_category1",
                "dim_base_category2",
                "dim_base_category3",
                "dim_base_dic",
                "dim_base_province",
                "dim_base_region",
                "dim_base_trademark",
                "dim_coupon_info",
                "dim_coupon_range",
                "dim_financial_sku_cost",
                "dim_sku_info",
                "dim_spu_info",
                "dim_user_info"
        };
        int count = 0;
        for (String tableName : tableNames) {
            try {
                int tablerows = countHbaseData("real_gmall", tableName);
                count=count+tablerows;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        System.out.println(count);
    }
}
