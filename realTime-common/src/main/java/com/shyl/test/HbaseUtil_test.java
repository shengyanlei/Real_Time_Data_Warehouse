package com.shyl.test;

import com.shyl.util.HbaseUtil;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;

public class HbaseUtil_test extends HbaseUtil {

    public static void main(String[] args) throws IOException {
        Connection hbaseConnection = getHbaseConnection();
        createHbaseTable(hbaseConnection,"default","user","userinfo");
//        dropHBaseTable(hbaseConnection,"default","test");
        closeHbaseConnection(hbaseConnection);
    }
}
