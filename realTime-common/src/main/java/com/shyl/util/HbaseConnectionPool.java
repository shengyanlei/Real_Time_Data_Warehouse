package com.shyl.util;


import org.apache.hadoop.hbase.client.Connection;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class HbaseConnectionPool {
    private static int maxSize = 10;
    private static int minSize = 5;
    private static ConcurrentLinkedQueue<Connection> connectionQueue = new ConcurrentLinkedQueue<>();
    private static Map<Connection, Long> connectionTimeMap = new ConcurrentHashMap<>();
    private static long maxIdleTime = 1000 * 60 * 60;

    public HbaseConnectionPool() {
    }

    public static Connection getConnection() throws InterruptedException {
        if (connectionQueue.size() < maxSize) {
            createConnections(minSize - connectionQueue.size());
        } else {
            Thread.sleep(1000); // Adjust the sleep time as needed
        }
        Connection connection = connectionQueue.poll();
        connectionTimeMap.put(connection, System.currentTimeMillis());
        return connection;
    }

    private static void createConnections(int count) {
        for (int i = 0; i < count; i++) {
            try {
                Connection connection = HbaseUtil.getHbaseConnection();
                connectionQueue.offer(connection);
                connectionTimeMap.put(connection, System.currentTimeMillis());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void closeConnection(Connection connection) throws IOException {
        HbaseUtil.closeHbaseConnection(connection);
        connectionTimeMap.remove(connection);
    }

    public static void retrunConnection(Connection connection) {
        if (connectionTimeMap.containsKey(connection) &&
                System.currentTimeMillis() - connectionTimeMap.get(connection) < maxIdleTime &&
                connectionQueue.size() < maxSize) {
            connectionQueue.offer(connection);
        } else {
            try {
                closeConnection(connection);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}

