package com.shyl.util;


import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

@Slf4j
public class HbaseUtil {
    //获取hbase连接
    public static Connection getHbaseConnection() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("hbase.zookeeper.quorum", "nodev2001");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        return ConnectionFactory.createConnection(configuration);
    }
//    关闭hbase连接
    public static void closeHbaseConnection(Connection hbaseconn) throws IOException {
        if (hbaseconn != null && !hbaseconn.isClosed()){
            hbaseconn.close();
        }
    }
//    创建hbase表
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
//表描述器
        TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
                .setColumnFamily(cfDesc)
                .build();
        admin.createTable(tableDescriptor);
        admin.close();
        log.info(namespace+" "+table+"建表成功");
    }
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

}
