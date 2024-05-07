package com.shyl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class testJSON {
    @Test
    public void test(){
        String json ="{\n" +
                "\t\"before\": null,\n" +
                "\t\"after\": {\n" +
                "\t\t\"id\": 1,\n" +
                "\t\t\"activity_name\": \"小米手机专场\",\n" +
                "\t\t\"activity_type\": \"3101\",\n" +
                "\t\t\"activity_desc\": \"小米手机满减2\",\n" +
                "\t\t\"start_time\": 1642035714000,\n" +
                "\t\t\"end_time\": 1687132800000,\n" +
                "\t\t\"create_time\": 1653609600000,\n" +
                "\t\t\"operate_time\": null\n" +
                "\t},\n" +
                "\t\"source\": {\n" +
                "\t\t\"version\": \"1.6.4.Final\",\n" +
                "\t\t\"connector\": \"mysql\",\n" +
                "\t\t\"name\": \"mysql_binlog_source\",\n" +
                "\t\t\"ts_ms\": 0,\n" +
                "\t\t\"snapshot\": \"false\",\n" +
                "\t\t\"db\": \"real_gmall\",\n" +
                "\t\t\"sequence\": null,\n" +
                "\t\t\"table\": \"activity_info\",\n" +
                "\t\t\"server_id\": 0,\n" +
                "\t\t\"gtid\": null,\n" +
                "\t\t\"file\": \"\",\n" +
                "\t\t\"pos\": 0,\n" +
                "\t\t\"row\": 0,\n" +
                "\t\t\"thread\": null,\n" +
                "\t\t\"query\": null\n" +
                "\t},\n" +
                "\t\"op\": \"r\",\n" +
                "\t\"ts_ms\": 1713942809934,\n" +
                "\t\"transaction\": null\n" +
                "}";

        JSONObject jsonObject = JSON.parseObject(json);
        System.out.println(jsonObject.keySet());
        System.out.println(JSON.parseObject(jsonObject.getString("after")).getString("id"));
    }

    @Test
    public void arraylist(){
        String a =null;
        System.out.println(Bytes.toBytes(a));
    }
}

