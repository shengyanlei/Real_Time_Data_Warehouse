package com.shyl.util;

import com.shyl.util.HbaseUtil;
import org.apache.hadoop.hbase.client.Connection;
import org.junit.Test;

import java.io.IOException;

public class HbaseUtil_test extends HbaseUtil {
    @Test
    public void testCreateHbaseTbale() throws IOException {
        Connection hbaseConnection = getHbaseConnection();
        createHbaseTable(hbaseConnection,"default","user1","userinfo");
//        dropHBaseTable(hbaseConnection,"default","test");
        closeHbaseConnection(hbaseConnection);
    }
    @Test
    public void testDropHBaseTable() throws IOException {
        Connection hbaseConnection = getHbaseConnection();
        dropHBaseTable(hbaseConnection,"default","user1");
        closeHbaseConnection(hbaseConnection);
    }
}
