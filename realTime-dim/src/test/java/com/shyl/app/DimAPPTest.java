package com.shyl.app;


import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.SocketTimeoutException;

public class DimAPPTest {


    @Test
    public void testFilterValidMySQLUpdate() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DimAPP dimApp = new DimAPP();
        String[] validJSONUpdate = {"{\"db\":\"mysql\",\"op\":\"u\",\"before\":{\"id\":1,\"name\":\"Alice\"},\"after\":{\"id\":1,\"name\":\"Bob\"}}"};
        DataStreamSource<String> source = env.fromElements(validJSONUpdate);
        Method filter = dimApp.getClass().getDeclaredMethod("filter", DataStreamSource.class);
        filter.setAccessible(true);
        Object result = filter.invoke(dimApp, source);
        ((SingleOutputStreamOperator<?>)result).print("woshi");
        env.execute();
    }

//    @Test
//    public void testFilterValidMySQLRead() throws Exception {
//        DimAPP dimApp = new DimAPP();
//        String[] validJSONRead = {"{\"db\":\"mysql\",\"op\":\"r\",\"before\":null,\"after\":{\"id\":1,\"name\":\"Alice\"}}"};
//        boolean result = dimApp.filter(validJSONRead);
//        Assert.assertTrue("Read operation on MySQL database should be true", result);
//    }
//
//    @Test
//    public void testFilterValidMySQLDelete() throws Exception {
//        DimAPP dimApp = new DimAPP();
//        String validJSONDelete = "{\"db\":\"mysql\",\"op\":\"d\",\"before\":{\"id\":1,\"name\":\"Alice\"},\"after\":null}";
//        boolean result = dimApp.filter(validJSONDelete);
//        Assert.assertTrue("Delete operation on MySQL database should be true", result);
//    }
//
//    @Test
//    public void testFilterInvalidJSON() throws Exception {
//        DimAPP dimApp = new DimAPP();
//        String invalidJSON = "{\"db\":\"mysql\",\"op\":\"unknown\",\"before\":{}}";
//        boolean result = dimApp.filter(invalidJSON);
//        Assert.assertFalse("Invalid JSON format should be false", result);
//    }
//
//    @Test
//    public void testFilterNonMySQLDatabase() throws Exception {
//        DimAPP dimApp = new DimAPP();
//        String nonMySQLJSON = "{\"db\":\"otherdb\",\"op\":\"r\",\"before\":null,\"after\":{\"id\":1,\"name\":\"Alice\"}}";
//        boolean result = dimApp.filter(nonMySQLJSON);
//        Assert.assertFalse("Operations on non-MySQL database should be false", result);
//    }
//
//    @Test
//    public void testFilterEmptyJSON() throws Exception {
//        DimAPP dimApp = new DimAPP();
//        String emptyJSON = "{}";
//        boolean result = dimApp.filter(emptyJSON);
//        Assert.assertFalse("Empty JSON input should be false", result);
//    }
//
    // Add more test cases if necessary...
}
