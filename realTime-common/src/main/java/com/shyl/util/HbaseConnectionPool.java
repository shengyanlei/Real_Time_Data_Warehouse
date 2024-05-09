package com.shyl.util;

import org.apache.hadoop.hbase.client.Connection;

import java.util.List;

public class HbaseConnectionPool {
    private static int maxSize = 10;
    private static int minSize = 5;
    private static List<Connection> connectionList;

    public HbaseConnectionPool() {
    }
    public static Connection getConnection() {
        if (connectionList.size() == 0) {
            synchronized (HbaseConnectionPool.class) {
                if (connectionList.size() == 0) {
                    for (int i = 0; i < minSize; i++) {
                        try {
                            connectionList.add(HbaseUtil.getHbaseConnection());
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }
        return connectionList.remove(0);
    }

    public static void closeConnection(Connection connection) {
        if (connectionList.size() < maxSize) {
            synchronized (HbaseConnectionPool.class) {
                connectionList.add(connection);
            }
        } else {
            try {
                HbaseUtil.closeHbaseConnection(connection);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    public static void retrunConnection(Connection connection) {
        if (connectionList.size() < maxSize) {
            synchronized (HbaseConnectionPool.class) {
                connectionList.add(connection);
            }
        } else {
            try {
                HbaseUtil.closeHbaseConnection(connection);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
