package com.shyl.util;

import com.google.common.base.CaseFormat;
import com.shyl.constant.Constant;
import org.apache.commons.beanutils.BeanUtils;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class JDBCUtil {
    public static Connection getMysqlConnection() throws ClassNotFoundException, SQLException {
        Class.forName(Constant.MYSQL_DRIVER);
        return DriverManager.getConnection(Constant.MYSQL_URL,Constant.MYSQL_USER_NAME,Constant.MYSQL_PASSWORD);

    }

    /**
     * 查询列表数据。
     * @param coon 数据库连接
     * @param Querysql SQL查询语句
     * @param tclass 需要映射的Java类类型
     * @param isUnderLineToCamel 是否将数据库字段的下划线命名转换为Java的驼峰命名方式，默认为false。如果传入true，则会进行转换。
     * @return 返回查询结果的列表，列表元素类型为tclass指定的类。
     * @throws SQLException 当执行SQL查询时发生错误
     * @throws InstantiationException 当尝试实例化tclass时发生错误
     * @throws IllegalAccessException 当尝试访问tclass的私有构造函数时发生错误
     * @throws InvocationTargetException 当调用tclass的构造函数时发生错误
     */
        public static <T> List<T> queryList(Connection coon,String Querysql,Class<T> tclass,boolean... isUnderLineToCamel) throws SQLException, InstantiationException, IllegalAccessException, InvocationTargetException {
            boolean defaultIsUToC =false; // 默认不执行下划线转驼峰
            if (isUnderLineToCamel.length>0){
                defaultIsUToC = isUnderLineToCamel[0];
            }

            List<T> result = new ArrayList<T>();

            PreparedStatement preparedStatement = coon.prepareStatement(Querysql); // 准备执行SQL语句
            ResultSet resultSet = preparedStatement.executeQuery(); // 执行查询并获取结果集
            ResultSetMetaData metaData = resultSet.getMetaData(); // 获取结果集的元数据

            // 遍历结果集，并将每条数据映射到tclass实例中
            while (resultSet.next()){
                T t = tclass.newInstance(); // 实例化tclass
                for (int i =1;i<=metaData.getColumnCount();i++){
                    String name = metaData.getColumnLabel(i); // 获取列标签
                    Object value = resultSet.getObject(i); // 获取列值
                    // 如果需要将下划线命名转换为驼峰命名，则进行转换
                    if (defaultIsUToC){
                        name = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, name);
                    }
                    BeanUtils.setProperty(t, name, value); // 将值设置到tclass实例中
                }
                result.add(t); // 将映射后的实例添加到结果列表
            }
            return  result; // 返回结果列表
        }

    public static void closeConnection(Connection conn) throws SQLException {
        if (conn != null && !conn.isClosed()){
            conn.close();
        }

    }
}
